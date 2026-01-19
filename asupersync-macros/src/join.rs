//! Implementation of the `join!` macro.
//!
//! The join macro runs multiple futures concurrently and waits for all
//! to complete, collecting their results into a tuple.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse::Parse, parse_macro_input, punctuated::Punctuated, Expr, Token};

/// Input to the join! macro: comma-separated futures
struct JoinInput {
    futures: Punctuated<Expr, Token![,]>,
}

impl Parse for JoinInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Self {
            futures: Punctuated::parse_terminated(input)?,
        })
    }
}

/// Generates the join implementation.
///
/// # Placeholder Implementation
///
/// This is a placeholder that will be fully implemented in `asupersync-mwff`.
/// Currently generates code that:
/// 1. Evaluates futures sequentially
/// 2. Returns a tuple of results
///
/// The full implementation will:
/// - Run all futures concurrently
/// - Combine outcomes using the severity lattice
/// - Support cancellation propagation
pub fn join_impl(input: TokenStream) -> TokenStream {
    let JoinInput { futures } = parse_macro_input!(input as JoinInput);

    let expanded = generate_join(&futures);
    TokenStream::from(expanded)
}

fn generate_join(futures: &Punctuated<Expr, Token![,]>) -> TokenStream2 {
    let future_count = futures.len();

    if future_count == 0 {
        return quote! { () };
    }

    // Generate bindings for each future
    let bindings: Vec<_> = futures
        .iter()
        .enumerate()
        .map(|(i, future)| {
            let ident = syn::Ident::new(
                &format!("__join_result_{i}"),
                proc_macro2::Span::call_site(),
            );
            quote! { let #ident = #future; }
        })
        .collect();

    // Generate the result tuple
    let results: Vec<_> = (0..future_count)
        .map(|i| {
            let ident = syn::Ident::new(
                &format!("__join_result_{i}"),
                proc_macro2::Span::call_site(),
            );
            quote! { #ident }
        })
        .collect();

    // Placeholder: evaluates sequentially
    // Full implementation will use concurrent polling
    quote! {
        {
            // Placeholder join implementation
            // Full implementation in asupersync-mwff will:
            // - Poll all futures concurrently
            // - Combine outcomes via severity lattice
            // - Support cancellation propagation
            #(#bindings)*
            (#(#results),*)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_future() {
        let input: proc_macro2::TokenStream = quote! { future_a };
        let parsed: JoinInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.futures.len(), 1);
    }

    #[test]
    fn test_parse_multiple_futures() {
        let input: proc_macro2::TokenStream = quote! { future_a, future_b, future_c };
        let parsed: JoinInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.futures.len(), 3);
    }

    #[test]
    fn test_parse_trailing_comma() {
        let input: proc_macro2::TokenStream = quote! { future_a, future_b, };
        let parsed: JoinInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.futures.len(), 2);
    }
}
