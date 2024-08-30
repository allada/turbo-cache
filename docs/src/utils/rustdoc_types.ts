// Generated from rustdoc-json-types via Claude 3.5 Sonnet. See:
// https://github.com/rust-lang/rust/blob/master/src/rustdoc-json-types/lib.rs

export const FORMAT_VERSION: number = 30;

export interface Crate {
  root: Id;
  crate_version: string | null;
  includes_private: boolean;
  index: Record<Id, Item>;
  paths: Record<Id, ItemSummary>;
  external_crates: Record<number, ExternalCrate>;
  format_version: number;
}

export interface ExternalCrate {
  name: string;
  html_root_url: string | null;
}

export interface ItemSummary {
  crate_id: number;
  path: string[];
  kind: ItemKind;
}

export interface Item {
  id: Id;
  crate_id: number;
  name: string | null;
  span: Span | null;
  visibility: Visibility;
  docs: string | null;
  links: Record<string, Id>;
  attrs: string[];
  deprecation: Deprecation | null;
  inner: ItemEnum;
}

export interface Span {
  filename: string;
  begin: [number, number];
  end: [number, number];
}

export interface Deprecation {
  since: string | null;
  note: string | null;
}

export type Visibility =
  | "public"
  | "default"
  | "crate"
  | { restricted: { parent: Id; path: string } };

export interface DynTrait {
  traits: PolyTrait[];
  lifetime: string | null;
}

export interface PolyTrait {
  trait: Path;
  generic_params: GenericParamDef[];
}

export type GenericArgs =
  | { angle_bracketed: { args: GenericArg[]; bindings: TypeBinding[] } }
  | { parenthesized: { inputs: Type[]; output: Type | null } };

export type GenericArg =
  | { lifetime: string }
  | { type: Type }
  | { const: Constant }
  | "infer";

export interface Constant {
  expr: string;
  value: string | null;
  is_literal: boolean;
}

export interface TypeBinding {
  name: string;
  args: GenericArgs;
  binding: TypeBindingKind;
}

export type TypeBindingKind =
  | { equality: Term }
  | { constraint: GenericBound[] };

export type Id = string;

export type ItemKind =
  | "module"
  | "extern_crate"
  | "import"
  | "struct"
  | "struct_field"
  | "union"
  | "enum"
  | "variant"
  | "function"
  | "type_alias"
  | "opaque_ty"
  | "constant"
  | "trait"
  | "trait_alias"
  | "impl"
  | "static"
  | "foreign_type"
  | "macro"
  | "proc_attribute"
  | "proc_derive"
  | "assoc_const"
  | "assoc_type"
  | "primitive"
  | "keyword";

export type ItemEnum =
  | { module: Module }
  | { extern_crate: { name: string; rename: string | null } }
  | { import: Import }
  | { union: Union }
  | { struct: Struct }
  | { struct_field: Type }
  | { enum: Enum }
  | { variant: Variant }
  | { function: Function }
  | { trait: Trait }
  | { trait_alias: TraitAlias }
  | { impl: Impl }
  | { type_alias: TypeAlias }
  | { opaque_ty: OpaqueTy }
  | { constant: { type: Type; const: Constant } }
  | { static: Static }
  | { foreign_type: Record<string, never> }
  | { macro: string }
  | { proc_macro: ProcMacro }
  | { primitive: Primitive }
  | { assoc_const: { type: Type; default: string | null } }
  | {
      assoc_type: {
        generics: Generics;
        bounds: GenericBound[];
        default: Type | null;
      };
    };

export interface Module {
  is_crate: boolean;
  items: Id[];
  is_stripped: boolean;
}

export interface Union {
  generics: Generics;
  fields_stripped: boolean;
  fields: Id[];
  impls: Id[];
}

export interface Struct {
  kind: StructKind;
  generics: Generics;
  impls: Id[];
}

export type StructKind =
  | "unit"
  | { tuple: (Id | null)[] }
  | { plain: { fields: Id[]; fields_stripped: boolean } };

export interface Enum {
  generics: Generics;
  variants_stripped: boolean;
  variants: Id[];
  impls: Id[];
}

export interface Variant {
  kind: VariantKind;
  discriminant: Discriminant | null;
}

export type VariantKind =
  | "plain"
  | { tuple: (Id | null)[] }
  | { struct: { fields: Id[]; fields_stripped: boolean } };

export interface Discriminant {
  expr: string;
  value: string;
}

export interface Header {
  const: boolean;
  unsafe: boolean;
  async: boolean;
  abi: Abi;
}

export type Abi =
  | "Rust"
  | { C: { unwind: boolean } }
  | { Cdecl: { unwind: boolean } }
  | { Stdcall: { unwind: boolean } }
  | { Fastcall: { unwind: boolean } }
  | { Aapcs: { unwind: boolean } }
  | { Win64: { unwind: boolean } }
  | { SysV64: { unwind: boolean } }
  | { System: { unwind: boolean } }
  | { Other: string };

export interface Function {
  decl: FnDecl;
  generics: Generics;
  header: Header;
  has_body: boolean;
}

export interface Generics {
  params: GenericParamDef[];
  where_predicates: WherePredicate[];
}

export interface GenericParamDef {
  name: string;
  kind: GenericParamDefKind;
}

export type GenericParamDefKind =
  | { lifetime: { outlives: string[] } }
  | {
      type: {
        bounds: GenericBound[];
        default: Type | null;
        synthetic: boolean;
      };
    }
  | { const: { type: Type; default: string | null } };

export type WherePredicate =
  | {
      bound_predicate: {
        type: Type;
        bounds: GenericBound[];
        generic_params: GenericParamDef[];
      };
    }
  | { region_predicate: { lifetime: string; bounds: GenericBound[] } }
  | { eq_predicate: { lhs: Type; rhs: Term } };

export type GenericBound =
  | {
      trait_bound: {
        trait: Path;
        generic_params: GenericParamDef[];
        modifier: TraitBoundModifier;
      };
    }
  | { outlives: string };

export type TraitBoundModifier = "none" | "maybe" | "maybe_const";

export type Term = { type: Type } | { constant: Constant };

export type Type =
  | { resolved_path: Path }
  | { dyn_trait: DynTrait }
  | { generic: string }
  | { primitive: string }
  | { function_pointer: FunctionPointer }
  | { tuple: Type[] }
  | { slice: Type }
  | { array: { type: Type; len: string } }
  | { pat: { type: Type; __pat_unstable_do_not_use: string } }
  | { impl_trait: GenericBound[] }
  | "infer"
  | { raw_pointer: { mutable: boolean; type: Type } }
  | { borrowed_ref: { lifetime: string | null; mutable: boolean; type: Type } }
  | {
      qualified_path: {
        name: string;
        args: GenericArgs;
        self_type: Type;
        trait: Path | null;
      };
    };

export interface Path {
  name: string;
  id: Id;
  args: GenericArgs | null;
}

export interface FunctionPointer {
  decl: FnDecl;
  generic_params: GenericParamDef[];
  header: Header;
}

export interface FnDecl {
  inputs: [string, Type][];
  output: Type | null;
  c_variadic: boolean;
}

export interface Trait {
  is_auto: boolean;
  is_unsafe: boolean;
  is_object_safe: boolean;
  items: Id[];
  generics: Generics;
  bounds: GenericBound[];
  implementations: Id[];
}

export interface TraitAlias {
  generics: Generics;
  params: GenericBound[];
}

export interface Impl {
  is_unsafe: boolean;
  generics: Generics;
  provided_trait_methods: string[];
  trait: Path | null;
  for: Type;
  items: Id[];
  negative: boolean;
  synthetic: boolean;
  blanket_impl: Type | null;
}

export interface Import {
  source: string;
  name: string;
  id: Id | null;
  glob: boolean;
}

export interface ProcMacro {
  kind: MacroKind;
  helpers: string[];
}

export type MacroKind = "bang" | "attr" | "derive";

export interface TypeAlias {
  type: Type;
  generics: Generics;
}

export interface OpaqueTy {
  bounds: GenericBound[];
  generics: Generics;
}

export interface Static {
  type: Type;
  mutable: boolean;
  expr: string;
}

export interface Primitive {
  name: string;
  impls: Id[];
}
