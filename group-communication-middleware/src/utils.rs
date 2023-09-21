#[macro_export]
macro_rules! impl_chainable_setter {
    ($(#[$($attrss:tt)*])* $field_name:ident, $input_type:ty) => {
        $(#[$($attrss)*])*
        pub fn $field_name(&mut self, $field_name: $input_type) -> &mut Self {
            self.$field_name = $field_name;
            self
        }
    };
}
