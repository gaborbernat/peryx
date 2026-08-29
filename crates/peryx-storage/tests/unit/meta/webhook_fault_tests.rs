use std::collections::HashSet;
use std::sync::Arc;

use redb::backends::InMemoryBackend;

use super::*;
use crate::meta::DRIVER_KV;
use crate::meta::fault::{self, Fault};

fn intent() -> WebhookEventIntent {
    WebhookEventIntent {
        index: "hosted".to_owned(),
        targets: vec!["audit".to_owned(), "deploy".to_owned()],
        event: "upload".to_owned(),
        payload: r#"{"event":"upload"}"#.to_owned(),
        created_at_unix: 10,
    }
}

fn seeded_store() -> (MetaStore, Arc<InMemoryBackend>, Arc<Fault>, String) {
    let (inner, fault) = fault::backend();
    let store = fault::create(&inner, &fault, |write| {
        write.open_table(SERIAL)?;
        write.open_table(WEBHOOK_DELIVERY)?;
        write.open_table(WEBHOOK_DUE)?;
        write.open_table(WEBHOOK_EVENT)?;
        Ok(())
    });
    store
        .commit_driver_txn(|txn| {
            txn.enqueue_webhook_event(intent());
            Ok::<_, MetaError>(((), Vec::new()))
        })
        .unwrap();
    let id = store.next_webhook_event_id().unwrap().unwrap();
    (store, inner, fault, id)
}

fn commit_mutation(store: &MetaStore) -> Result<(), MetaError> {
    store.commit_driver_txn(|txn| {
        if txn.upsert("mutation", b"committed")? {
            txn.enqueue_webhook_event(intent());
        }
        Ok(((), Vec::new()))
    })
}

#[test]
fn test_webhook_intent_commits_atomically_with_its_mutation_across_backend_failures() {
    let mut failures = 0;
    for fail_after in 0..256 {
        let (inner, fault) = fault::backend();
        let store = fault::create(&inner, &fault, |write| {
            write.open_table(SERIAL)?;
            write.open_table(DRIVER_KV)?;
            write.open_table(WEBHOOK_DELIVERY)?;
            write.open_table(WEBHOOK_DUE)?;
            write.open_table(WEBHOOK_EVENT)?;
            Ok(())
        });
        fault.arm(fail_after);
        if commit_mutation(&store).is_err() {
            failures += 1;
            fault.disable();
            drop(store);
            let store = fault::reopen(&inner, &fault);
            assert_eq!(
                store.get_driver_value("mutation").unwrap().is_some(),
                store.next_webhook_event_id().unwrap().is_some()
            );
            commit_mutation(&store).unwrap();
            while let Some(event) = store.next_webhook_event_id().unwrap() {
                store.fan_out_webhook_event(&event).unwrap();
            }
            let deliveries = store.list_webhook_deliveries().unwrap();
            assert_eq!(deliveries.len(), 2);
            assert_eq!(
                deliveries
                    .iter()
                    .map(|delivery| delivery.target.as_str())
                    .collect::<HashSet<_>>(),
                HashSet::from(["audit", "deploy"])
            );
        }
    }
    assert!(failures > 0);
}

fn assert_targets(deliveries: &[WebhookDeliveryRecord]) {
    assert_eq!(deliveries.len(), 2);
    assert_eq!(
        deliveries
            .iter()
            .map(|delivery| delivery.target.as_str())
            .collect::<HashSet<_>>(),
        HashSet::from(["audit", "deploy"])
    );
}

fn assert_recovered(store: &MetaStore, event_id: &str, retained: &[String]) {
    assert!(store.fan_out_webhook_event(event_id).unwrap());
    assert_eq!(store.next_webhook_event_id().unwrap(), None);
    let deliveries = store.list_webhook_deliveries().unwrap();
    assert_targets(&deliveries);
    assert!(
        retained
            .iter()
            .all(|id| deliveries.iter().any(|delivery| &delivery.id == id))
    );
}

#[test]
fn test_webhook_event_recovers_first_and_later_queue_write_failures_without_duplicates() {
    let mut recovered_counts = HashSet::new();
    let mut committed_after_error = false;
    for fail_after in 0..256 {
        let (store, inner, fault, event_id) = seeded_store();
        fault.arm(fail_after);
        store.fan_out_webhook_event(&event_id).unwrap_err();
        fault.disable();
        drop(store);
        let store = fault::reopen(&inner, &fault);
        let deliveries = store.list_webhook_deliveries().unwrap();
        if let Some(pending) = store.next_webhook_event_id().unwrap() {
            assert_eq!(pending, event_id);
            let retained = deliveries
                .iter()
                .map(|delivery| delivery.id.clone())
                .collect::<Vec<_>>();
            recovered_counts.insert(deliveries.len());
            if let Some(delivery) = deliveries.first() {
                store
                    .update_webhook_delivery(
                        &delivery.id,
                        WebhookDeliveryAttempt {
                            status: WebhookDeliveryStatus::Delivered,
                            updated_at_unix: 11,
                            next_attempt_at_unix: None,
                            response_status: Some(204),
                            last_error: None,
                        },
                    )
                    .unwrap();
            }
            assert_recovered(&store, &event_id, &retained);
        } else {
            assert_targets(&deliveries);
            committed_after_error = true;
        }
        if recovered_counts == HashSet::from([0, 1, 2]) && committed_after_error {
            break;
        }
    }
    assert_eq!(recovered_counts, HashSet::from([0, 1, 2]));
    assert!(committed_after_error);
}
