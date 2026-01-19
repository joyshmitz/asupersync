//! Implementation of the `spawn!` macro.
//!
//! The spawn macro creates a task owned by the enclosing region.
//! The task cannot orphan and will be cancelled when the region closes.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse::Parse, parse_macro_input, Expr};

/// Input to the spawn! macro: an async expression
struct SpawnInput {
    future: Expr,
}

impl Parse for SpawnInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Self {
            future: input.parse()?,
        })
    }
}

/// Generates the spawn implementation.
///
/// # Placeholder Implementation
///
/// This is a placeholder that will be fully implemented in `asupersync-5tic`.
/// Currently generates code that:
/// 1. Returns the future expression directly
///
/// The full implementation will:
/// - Call `scope.spawn()` with the future
/// - Return a `TaskHandle` for the spawned task
/// - Ensure proper ownership by the region
pub fn spawn_impl(input: TokenStream) -> TokenStream {
    let SpawnInput { future } = parse_macro_input!(input as SpawnInput);

    let expanded = generate_spawn(&future);
    TokenStream::from(expanded)
}

fn generate_spawn(future: &Expr) -> TokenStream2 {
    // Placeholder: returns future directly
    // Full implementation will call scope.spawn()
    quote! {
        {
            // Placeholder spawn implementation
            // Full implementation in asupersync-5tic will:
            // - Get scope from ambient context
            // - Call scope.spawn(future)
            // - Return TaskHandle
            #future
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_spawn_input() {
        let input: proc_macro2::TokenStream = quote! { async { 42 } };
        let parsed: SpawnInput = syn::parse2(input).unwrap();
        assert!(matches!(parsed.future, Expr::Async(_)));
    }

    #[test]
    fn test_parse_spawn_input_with_move() {
        let input: proc_macro2::TokenStream = quote! { async move { captured } };
        let parsed: SpawnInput = syn::parse2(input).unwrap();
        assert!(matches!(parsed.future, Expr::Async(_)));
    }
}
