import type {
  Crate,
  Enum,
  GenericArg,
  GenericArgs,
  Id,
  Item,
  Path,
  Struct,
  Type,
  TypeBinding,
  Variant,
} from "./rustdoc_types";

type JsonExample =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonExample }
  | JsonExample[];

const isResolvedPathType = (
  type: Type,
): type is Extract<Type, { resolved_path: Path }> => {
  return typeof type === "object" && "resolved_path" in type;
};

const isPrimitiveType = (
  type: Type,
): type is Extract<Type, { primitive: string }> => {
  return typeof type === "object" && "primitive" in type;
};

const isAngleBracketedArgs = (
  args: GenericArgs | null,
): args is Extract<
  NonNullable<GenericArgs>,
  { angle_bracketed: { args: GenericArg[]; bindings: TypeBinding[] } }
> => {
  return (
    args !== null &&
    "angle_bracketed" in args &&
    Array.isArray(args.angle_bracketed.args)
  );
};

function isTypeArg(arg: GenericArg): arg is { type: Type } {
  return typeof arg === "object" && "type" in arg;
}

const isStructItem = (
  item: Item,
): item is Extract<Item, { inner: { struct: Struct } }> => {
  return "struct" in item.inner;
};

const isValidStructField = (
  item: Item & { inner: { struct: Struct } },
): item is Item & {
  inner: { struct: Struct & { kind: { plain: { fields: Id[] } } } };
} => {
  return (
    typeof item.inner.struct.kind === "object" &&
    "plain" in item.inner.struct.kind &&
    Array.isArray(item.inner.struct.kind.plain.fields)
  );
};

const isEnumItem = (
  item: Item,
): item is Extract<Item, { inner: { enum: Enum } }> => {
  return "enum" in item.inner;
};

const removePrefix = (name: string): string => {
  return name
    .replace(/^(crate::)?stores::/, "")
    .replace(/^std::collections::/, "");
};

const generatePrimitiveJsonExample = (type: {
  primitive: string;
}): JsonExample => {
  switch (type.primitive) {
    case "String":
      return "example_string";
    case "bool":
      return true;
    case "u32":
    case "u64":
    case "usize":
      return 42;
    case "f32":
      return 3.14;
    default:
      return 0;
  }
};

const generateHashMapJsonExample = (
  args: GenericArgs,
  crate: Crate,
  depth: number,
): JsonExample => {
  if (isAngleBracketedArgs(args) && args.angle_bracketed.args.length >= 2) {
    const keyArg = args.angle_bracketed.args[0];
    const valueArg = args.angle_bracketed.args[1];
    if (
      keyArg !== undefined &&
      valueArg !== undefined &&
      isTypeArg(keyArg) &&
      isTypeArg(valueArg)
    ) {
      const key = generateJsonExample(keyArg.type, crate, depth + 1);
      const value = generateJsonExample(valueArg.type, crate, depth + 1);
      return [{ [String(key)]: value }];
    }
  }
  return []; // Return an empty array if the structure is not as expected
};

const generateDefaultJsonExample = (
  id: Id | null,
  crate: Crate,
  depth: number,
  cleanName: string,
): JsonExample => {
  if (id && crate.index[id]) {
    const item = crate.index[id];
    if (item && isEnumItem(item)) {
      return generateEnumJsonExample(item, crate, depth + 1);
    }
    if (item && isStructItem(item)) {
      return generateStructJsonExample(item, crate, depth + 1);
    }
    if (item && "type_alias" in item.inner) {
      return generateJsonExample(item.inner.type_alias.type, crate, depth + 1);
    }
  }
  return cleanName;
};

const generateResolvedPathJsonExample = (
  type: Extract<Type, { resolved_path: Path }>,
  crate: Crate,
  depth: number,
): JsonExample => {
  const { name, id, args } = type.resolved_path;
  const cleanName = removePrefix(name);
  switch (cleanName) {
    case "Option":
      return null;
    case "Vec":
      return [];
    case "String":
      return "example_string";
    case "HashMap": {
      if (isAngleBracketedArgs(args)) {
        return generateHashMapJsonExample(args, crate, depth);
      }
      return [];
    }
    default: {
      return generateDefaultJsonExample(id, crate, depth, cleanName);
    }
  }
};

const generateJsonExample = (
  type: Type,
  crate: Crate,
  depth = 0,
): JsonExample => {
  if (depth > 10) {
    return "Max depth reached";
  }

  if (isPrimitiveType(type)) {
    return generatePrimitiveJsonExample(type);
  }
  if (isResolvedPathType(type)) {
    return generateResolvedPathJsonExample(type, crate, depth);
  }
  return "Unknown";
};

const removeKeyQuotes = (jsonString: string): string => {
  return jsonString.replace(/"([^"]+)":/g, "$1:");
};

const customStringify = (obj: JsonExample, indent = 2): string => {
  const jsonString = JSON.stringify(obj, null, indent);
  return removeKeyQuotes(jsonString);
};

const generateEnumJsonExample = (
  item: Item,
  crate: Crate,
  depth: number,
): JsonExample => {
  if ("enum" in item.inner) {
    const variants = item.inner.enum.variants.map(
      (variantId: Id) => crate.index[variantId],
    );
    const example = variants[0]; // Take the first variant as an example
    if (example) {
      return generateVariantJsonExample(example, crate, depth);
    }
  }
  return "Unknown";
};

const generateVariantJsonExample = (
  variant: Item,
  crate: Crate,
  depth: number,
): JsonExample => {
  if ("variant" in variant.inner) {
    const variantInner = variant.inner.variant;
    if (
      variantInner.kind &&
      typeof variantInner.kind === "object" &&
      "tuple" in variantInner.kind &&
      Array.isArray(variantInner.kind.tuple) &&
      variantInner.kind.tuple.length > 0
    ) {
      const fieldId = variantInner.kind.tuple[0];
      if (
        fieldId !== null &&
        typeof fieldId === "string" &&
        fieldId in crate.index
      ) {
        const field = crate.index[fieldId];
        if (field && "struct_field" in field.inner) {
          const fieldType = field.inner.struct_field;
          const value = generateJsonExample(fieldType, crate, depth + 1);
          return { [variant.name || ""]: value };
        }
      }
    }
  }
  return variant.name || "";
};

const generateFieldExample = (
  fieldId: Id,
  crate: Crate,
  depth: number,
): [string, JsonExample] | null => {
  if (fieldId in crate.index) {
    const field = crate.index[fieldId];
    if (field && "struct_field" in field.inner && field.name) {
      const fieldType = field.inner.struct_field;
      return [field.name, generateJsonExample(fieldType, crate, depth + 1)];
    }
  }
  return null;
};

const generateStructJsonExample = (
  item: Item & { inner: { struct: Struct } },
  crate: Crate,
  depth: number,
): JsonExample => {
  const example: Record<string, JsonExample> = {};
  if (isValidStructField(item)) {
    for (const fieldId of item.inner.struct.kind.plain.fields) {
      const fieldExample = generateFieldExample(fieldId, crate, depth);
      if (fieldExample) {
        const [fieldName, fieldValue] = fieldExample;
        example[fieldName] = fieldValue;
      }
    }
  }
  return example;
};

const escapeMarkdown = (text: string): string =>
  text.replace(/([\\*`_{}[\]()#+\-.!])/g, "\\$1");

const transformUrls = (text: string): string =>
  text.replace(/<(https?:\/\/[^>]+)>/g, (_, url) => `[${url}](${url})`);

const processDocText = (text: string): string => {
  const blocks = text.split(/(```[\s\S]*?```)/);

  return blocks
    .map((block) => {
      if (block.startsWith("```") && block.endsWith("```")) {
        return block;
      }
      let processed = escapeMarkdown(block);
      processed = transformUrls(processed);
      processed = processed.replace(/<([^>]+)>/g, "`$1`");
      return processed;
    })
    .join("");
};

const generateHeadingId = (heading: string): string => {
  return heading
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
};

const generateTypeDescription = (type: Type): string => {
  if (isPrimitiveType(type)) {
    return `(${type.primitive})`;
  }

  if (isResolvedPathType(type)) {
    const { name, args } = type.resolved_path;

    const getInnerType = (index: number): string => {
      if (args && "angle_bracketed" in args) {
        const arg = args.angle_bracketed.args[index];
        if (arg && isTypeArg(arg)) {
          return generateTypeDescription(arg.type).replace(/^\(|\)$/g, "");
        }
      }
      return "Unknown";
    };

    const finalTypeName = removePrefix(name.split("::").pop() || name);

    switch (finalTypeName) {
      case "Option":
        return `(optional ${getInnerType(0)})`;
      case "Vec":
        return `(list of ${getInnerType(0)})`;
      case "Box":
        return `(box of ${getInnerType(0)})`;
      case "String":
        return "";
      case "HashMap":
        return `(list of objects ${getInnerType(0)}: ${getInnerType(1)})`;
      default: {
        const headingId = generateHeadingId(finalTypeName);
        return `([${finalTypeName}](#${headingId}))`;
      }
    }
  }

  return "(Unknown)";
};

const generateEnumHeader = (item: Item, crate: Crate): string => {
  let content = `### ${escapeMarkdown(item.name || "")}\n\n`;
  const example = generateEnumJsonExample(item, crate, 0);
  content += "```json5\n";
  content += customStringify(example, 2);
  content += "\n```\n\n";
  if (item.docs) {
    content += `${processDocText(item.docs)}\n\n`;
  }
  return content;
};

const generateVariantTypeDescription = (
  variantInner: Variant,
  crate: Crate,
): string => {
  if (
    variantInner.kind &&
    typeof variantInner.kind === "object" &&
    "tuple" in variantInner.kind
  ) {
    const tupleFields = variantInner.kind.tuple
      .filter((fieldId): fieldId is Id => fieldId !== null)
      .map((fieldId) => {
        const field = crate.index[fieldId];
        if (field && "struct_field" in field.inner) {
          const fieldType = field.inner.struct_field;
          return generateTypeDescription(fieldType);
        }
        return "Unknown";
      });
    if (tupleFields.length > 0) {
      return ` ${tupleFields.join(", ")}`;
    }
  }
  return "";
};

const generateVariantContent = (variant: Item, crate: Crate): string => {
  if ("variant" in variant.inner) {
    const variantInner = variant.inner.variant;
    let variantContent = `- \`${variant.name || ""}\``;
    variantContent += generateVariantTypeDescription(variantInner, crate);
    if (variant.docs) {
      variantContent += `: ${processDocText(variant.docs)}`;
    }
    return `${variantContent}\n\n`;
  }
  return "";
};

const generateEnumVariants = (item: Item, crate: Crate): string => {
  let content = "**Variants**\n\n";
  if ("enum" in item.inner) {
    for (const variantId of item.inner.enum.variants) {
      const variant = crate.index[variantId];
      if (variant) {
        content += generateVariantContent(variant, crate);
      }
    }
  }
  return content;
};

const generateEnumContent = (
  item: Extract<Item, { inner: { enum: Enum } }>,
  crate: Crate,
): string => {
  let content = generateEnumHeader(item, crate);
  content += generateEnumVariants(item, crate);
  return content;
};

const generateStructHeader = (
  item: Item & { inner: { struct: Struct } },
  crate: Crate,
): string => {
  let content = `### ${escapeMarkdown(item.name || "")}\n\n`;
  const example = generateStructJsonExample(item, crate, 0);
  content += "```json5\n";
  content += customStringify(example, 2);
  content += "\n```\n\n";
  if (item.docs) {
    content += `${processDocText(item.docs)}\n\n`;
  }
  return content;
};

const generateFieldContent = (field: Item, fieldType: Type): string => {
  const fieldName = field.name || "";
  const fieldDocs = field.docs || "No description";
  return `- \`${fieldName}\` ${generateTypeDescription(fieldType)}: ${processDocText(fieldDocs)}\n\n`;
};

const generateStructFields = (
  item: Item & { inner: { struct: Struct } },
  crate: Crate,
): string => {
  let content = "**Fields**\n\n";
  if (isValidStructField(item)) {
    for (const fieldId of item.inner.struct.kind.plain.fields) {
      const field = crate.index[fieldId];
      if (field && "struct_field" in field.inner) {
        const fieldType = field.inner.struct_field;
        content += generateFieldContent(field, fieldType);
      }
    }
  }
  return content;
};

const generateStructContent = (
  item: Extract<Item, { inner: { struct: Struct } }>,
  crate: Crate,
): string => {
  let content = generateStructHeader(item, crate);

  if (isValidStructField(item)) {
    content += generateStructFields(item, crate);
  }

  return content;
};

const generateTypeAliasContent = (item: Item): string => {
  let content = `### ${escapeMarkdown(item.name || "")}\n\n`;

  if ("type_alias" in item.inner) {
    const aliasedType = item.inner.type_alias.type;
    content += `${generateTypeDescription(aliasedType)}\n\n`;
  }

  if (item.docs) {
    content += `${processDocText(item.docs)}\n\n`;
  }

  return content;
};

const generateModuleHeader = (moduleItem: Item, depth: number): string => {
  let content = `${"#".repeat(depth)} ${escapeMarkdown(moduleItem.name || "")}\n\n`;
  if (moduleItem.docs) {
    content += `${processDocText(moduleItem.docs)}\n\n`;
  }
  return content;
};

const generateItemContent = (
  item: Item,
  crate: Crate,
  depth: number,
): string => {
  if ("type_alias" in item.inner) {
    return generateTypeAliasContent(item);
  }
  if (isStructItem(item)) {
    return generateStructContent(item, crate);
  }
  if (isEnumItem(item)) {
    return generateEnumContent(item, crate);
  }
  if ("module" in item.inner) {
    return generateModuleContent(item, crate, depth + 1);
  }
  return "";
};

const generateModuleItems = (
  moduleItem: Item,
  crate: Crate,
  depth: number,
): string => {
  let content = "";
  if ("module" in moduleItem.inner) {
    for (const itemId of moduleItem.inner.module.items) {
      const item = crate.index[itemId];
      if (item) {
        content += generateItemContent(item, crate, depth);
      }
    }
  }
  return content;
};

const generateModuleContent = (
  moduleItem: Item,
  crate: Crate,
  depth = 1,
): string => {
  let moduleContent = generateModuleHeader(moduleItem, depth);
  moduleContent += generateModuleItems(moduleItem, crate, depth);
  return moduleContent;
};

export const generateAstroContent = (crate: Crate): string => {
  let astroContent = `---
title: NativeLink Configuration
description: The NativeLink Configuration Reference
---

:::caution
This page is auto-generated and may contain some minor formatting errors.
If you find any, please feel free to open an issue in our [open-source repository](https://github.com/TraceMachina/nativelink/issues).
:::

This page documents the configuration options for NativeLink.

`;

  const rootModule = crate.index[crate.root];
  if (rootModule && "module" in rootModule.inner) {
    astroContent += generateModuleContent(rootModule, crate);
  }

  return astroContent;
};
