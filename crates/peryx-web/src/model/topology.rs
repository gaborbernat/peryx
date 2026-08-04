//! Presentation helpers over [`peryx_core::TopologySnapshot`], the availability-topology DTO the
//! server projects and the browser deserializes unchanged. The snapshot already carries only the
//! fields the caller's class admits, so these helpers add labels and a role filter, never authority.

use peryx_core::{NodeLiveness, NodeRole, TopologyMode};

pub use peryx_core::{LocalNode, TopologyNode, TopologySnapshot};

/// A health cell: the text an operator reads plus the css class that tints it. The text stands on its
/// own so a color-blind reader loses nothing, and a withheld field reads as restricted, never healthy.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HealthLabel {
    pub text: &'static str,
    pub class: &'static str,
}

/// A node's liveness as a labelled health cell. `None` means the caller's class did not admit the
/// field, so it reads `restricted` rather than borrowing a healthy look it was not granted.
#[must_use]
pub fn liveness_health(liveness: Option<NodeLiveness>) -> HealthLabel {
    match liveness {
        Some(NodeLiveness::Live) => HealthLabel {
            text: "Live",
            class: "health-live",
        },
        Some(NodeLiveness::Unready) => HealthLabel {
            text: "Unready",
            class: "health-unready",
        },
        Some(NodeLiveness::Unknown) => HealthLabel {
            text: "Unknown",
            class: "health-unknown",
        },
        None => HealthLabel {
            text: "Restricted",
            class: "health-restricted",
        },
    }
}

#[must_use]
pub const fn role_label(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Writer => "Writer",
        NodeRole::Replica => "Replica",
    }
}

#[must_use]
pub const fn mode_label(mode: TopologyMode) -> &'static str {
    match mode {
        TopologyMode::None => "Single node",
        TopologyMode::Dc => "Datacenter",
        TopologyMode::Ha => "High availability",
    }
}

/// The roster filter a reader drives from the role select. `All` is the default so the accessible,
/// script-free render shows the whole roster.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum RoleFilter {
    #[default]
    All,
    Writer,
    Replica,
}

impl RoleFilter {
    #[must_use]
    pub fn from_value(value: &str) -> Self {
        match value {
            "writer" => Self::Writer,
            "replica" => Self::Replica,
            _ => Self::All,
        }
    }

    #[must_use]
    pub fn matches(self, role: NodeRole) -> bool {
        match self {
            Self::All => true,
            Self::Writer => role == NodeRole::Writer,
            Self::Replica => role == NodeRole::Replica,
        }
    }
}

#[cfg(test)]
mod tests {
    use peryx_core::{NodeLiveness, NodeRole, TopologyMode};
    use rstest::rstest;

    use super::{RoleFilter, liveness_health, mode_label, role_label};

    #[rstest]
    #[case(Some(NodeLiveness::Live), "Live", "health-live")]
    #[case(Some(NodeLiveness::Unready), "Unready", "health-unready")]
    #[case(Some(NodeLiveness::Unknown), "Unknown", "health-unknown")]
    #[case(None, "Restricted", "health-restricted")]
    fn test_liveness_health_labels_every_state_with_text(
        #[case] liveness: Option<NodeLiveness>,
        #[case] text: &str,
        #[case] class: &str,
    ) {
        let label = liveness_health(liveness);
        assert_eq!(label.text, text);
        assert_eq!(label.class, class);
    }

    #[test]
    fn test_withheld_liveness_never_reads_as_healthy() {
        assert_eq!(liveness_health(None).text, "Restricted");
        assert_ne!(
            liveness_health(None).class,
            liveness_health(Some(NodeLiveness::Live)).class
        );
    }

    #[rstest]
    #[case(NodeRole::Writer, "Writer")]
    #[case(NodeRole::Replica, "Replica")]
    fn test_role_label(#[case] role: NodeRole, #[case] label: &str) {
        assert_eq!(role_label(role), label);
    }

    #[rstest]
    #[case(TopologyMode::None, "Single node")]
    #[case(TopologyMode::Dc, "Datacenter")]
    #[case(TopologyMode::Ha, "High availability")]
    fn test_mode_label(#[case] mode: TopologyMode, #[case] label: &str) {
        assert_eq!(mode_label(mode), label);
    }

    #[rstest]
    #[case("writer", RoleFilter::Writer)]
    #[case("replica", RoleFilter::Replica)]
    #[case("all", RoleFilter::All)]
    #[case("", RoleFilter::All)]
    #[case("nonsense", RoleFilter::All)]
    fn test_role_filter_parses_value(#[case] value: &str, #[case] filter: RoleFilter) {
        assert_eq!(RoleFilter::from_value(value), filter);
    }

    #[rstest]
    #[case(RoleFilter::All, NodeRole::Writer, true)]
    #[case(RoleFilter::All, NodeRole::Replica, true)]
    #[case(RoleFilter::Writer, NodeRole::Writer, true)]
    #[case(RoleFilter::Writer, NodeRole::Replica, false)]
    #[case(RoleFilter::Replica, NodeRole::Replica, true)]
    #[case(RoleFilter::Replica, NodeRole::Writer, false)]
    fn test_role_filter_matches(#[case] filter: RoleFilter, #[case] role: NodeRole, #[case] matches: bool) {
        assert_eq!(filter.matches(role), matches);
    }
}
