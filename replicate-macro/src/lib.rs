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
                .parse2(quote! { #[prost(uint64, tag = "1")] pub correlation_id: u64 })
                .unwrap(),
        );
    }

    return quote! {#item_struct}.into();
}