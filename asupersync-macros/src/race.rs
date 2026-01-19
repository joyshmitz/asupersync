//! Implementation of the `race!` macro.
//!
//! The race macro runs multiple futures concurrently and returns the result
//! of the first to complete. Losers are automatically cancelled and drained.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse::Parse, parse_macro_input, punctuated::Punctuated, Expr, Token};

/// Input to the race! macro: comma-separated futures
struct RaceInput {
    futures: Punctuated<Expr, Token![,]>,
}

impl Parse for RaceInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Self {
            futures: Punctuated::parse_terminated(input)?,
        })
    }
}

/// Generates the race implementation.
///
/// # Placeholder Implementation
///
/// This is a placeholder that will be fully implemented in `asupersync-hcpl`.
/// Currently generates code that:
/// 1. Returns the first future's result directly
///
/// The full implementation will:
/// - Poll all futures concurrently
/// - Return when first completes
/// - Cancel and drain all losers
/// - Ensure no orphaned work
pub fn race_impl(input: TokenStream) -> TokenStream {
    let RaceInput { futures } = parse_macro_input!(input as RaceInput);

    let expanded = generate_race(&futures);
    TokenStream::from(expanded)
}

fn generate_race(futures: &Punctuated<Expr, Token![,]>) -> TokenStream2 {
    if futures.is_empty() {
        return quote! {
            compile_error!("race! requires at least one future")
        };
    }

    // For placeholder, just return the first future
    let first = futures.first().unwrap();

    // Placeholder: returns first future only
    // Full implementation will race all futures
    quote! {
        {
            // Placeholder race implementation
            // Full implementation in asupersync-hcpl will:
            // - Poll all futures concurrently
            // - Return winner's result
            // - Cancel and drain all losers
            // - Guarantee no orphaned work
            #first
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_future() {
        let input: proc_macro2::TokenStream = quote! { future_a };
        let parsed: RaceInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.futures.len(), 1);
    }

    #[test]
    fn test_parse_multiple_futures() {
        let input: proc_macro2::TokenStream = quote! { future_a, future_b };
        let parsed: RaceInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.futures.len(), 2);
    }

    #[test]
    fn test_empty_race_error() {
        let futures: Punctuated<Expr, Token![,]> = Punctuated::new();
        let result = generate_race(&futures);
        let result_str = result.to_string();
        assert!(result_str.contains("compile_error"));
    }
}
