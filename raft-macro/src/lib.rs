use proc_macro::TokenStream;

use quote::quote;
use syn::{ItemStruct, parse, parse_macro_input};
use syn::parse::Parser;

#[proc_macro_attribute]
pub fn add_correlation_id(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item_struct = parse_macro_input!(input as ItemStruct);
    let _ = parse_macro_input!(args as parse::Nothing);

    if let syn::Fields::Named(ref mut fields) = item_struct.fields {
        fields.named.push(
            syn::Field::parse_named
                .parse2(quote! { #[prost(int64, tag = "1")] pub request_id: i64 })
                .unwrap(),
        );
        fields.named.push(
            syn::Field::parse_named
                .parse2(quote! { #[prost(int64, tag = "2")] pub correlation_id: i64 })
                .unwrap(),
        );
    }

    return quote! {#item_struct}.into();
}