use leptos::prelude::*;

use crate::data::load_login;
use crate::model::UiLoginState;

/// The browser login page: a signed-in banner with logout, or the list of OIDC providers to sign in
/// with. It reads its state through a `Suspense` so a no-JS client still receives the resolved page.
#[component]
pub fn Login() -> impl IntoView {
    let state = Resource::new(|| (), |()| load_login());
    view! {
        <section class="page">
            <Suspense fallback=|| view! { <p class="dim">"loading"</p> }>
                {move || Suspend::new(async move { login_view(state.await) })}
            </Suspense>
        </section>
    }
}

/// Render the login surface from resolved state.
fn login_view(state: UiLoginState) -> impl IntoView {
    view! {
        <h1>"Sign in"</h1>
        {match state.user {
            Some(name) => view! {
                <p>"Signed in as " <strong>{name}</strong>"."</p>
                <form method="post" action="/_/logout">
                    <button type="submit">"Log out"</button>
                </form>
            }
            .into_any(),
            None if state.providers.is_empty() => {
                view! { <p class="dim">"No login providers are configured."</p> }.into_any()
            }
            None => view! {
                <p>"Choose a provider to sign in to the dashboard."</p>
                <ul class="provider-list">
                    {state
                        .providers
                        .into_iter()
                        .map(|provider| {
                            let href = format!("/_/login/{provider}");
                            view! {
                                <li>
                                    <a class="button" href=href>
                                        "Sign in with " {provider}
                                    </a>
                                </li>
                            }
                        })
                        .collect_view()}
                </ul>
            }
            .into_any(),
        }}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_login_view_lists_a_sign_in_link_per_provider() {
        let html = login_view(UiLoginState {
            user: None,
            providers: vec!["corporate".to_owned(), "google".to_owned()],
        })
        .to_html();
        assert!(html.contains("/_/login/corporate"), "{html}");
        assert!(html.contains("/_/login/google"), "{html}");
    }

    #[test]
    fn test_login_view_shows_the_signed_in_user_and_a_logout_form() {
        let html = login_view(UiLoginState {
            user: Some("Ada Lovelace".to_owned()),
            providers: Vec::new(),
        })
        .to_html();
        assert!(html.contains("Ada Lovelace"), "{html}");
        assert!(html.contains("/_/logout"), "{html}");
    }

    #[test]
    fn test_login_view_without_providers_reports_none_configured() {
        let html = login_view(UiLoginState::default()).to_html();
        assert!(html.contains("No login providers are configured."), "{html}");
    }
}
