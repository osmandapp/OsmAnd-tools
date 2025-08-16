#Spec: Nashorn-compatible rule functions (JDK 8–14)

This spec defines how to write small, pure JavaScript rule functions that will run on a Java server via Nashorn 
(JDK 8–14). The host system will load one .js file and discover/export functions which have two or more parameters 
with specified names.
1) Runtime & language constraints
- Engine: Oracle Nashorn, JDK 8–14.
- ECMAScript level: Write in ES5 syntax for maximum compatibility.
        You MAY use Array.prototype methods, regexes, JSON, etc.
        Avoid arrow functions, default parameters, spread/rest, modules, Promise, async/await, and Node APIs.
        Use "use strict"; at the top of each file.
- Environment: No DOM, no require, no network access. Standard JS globals only (Math, Date, etc.).
- Java interop: Not required. If absolutely needed, prefer Java.type(...) guarded for compatibility.

2) Function discovery & naming
- Example: function joinColumns(row, columns, separator) { ... }
- Do not attach functions to objects; do not use modules/exports.

3) Function signature & parameters
Every exposed function MUST follow this order:
- function something(row, columns, param1, ..., paramN) -> string[]
- row (required): an object representing a single record. Values are typically strings (from CSV/Overpass), but may 
  be numbers or null/undefined. Access by key: row["colName"].
- columns (required): an ordered array of strings listing the column names present in row. Example: 
   ["house_number", "street", "city"].
- Optional parameters: primitive only (string or number). No arrays/objects/functions. If you need defaults, 
implement them inside the function body (ES5 style):
        separator = (typeof separator === "string") ? separator : ", ";
- Return type: string[].

Parameter validation
Inside each function, validate inputs defensively:
- Throw Error("...") on programmer errors (wrong types for required params).
- For data issues (missing values, empty columns) prefer returning an empty array rather than throwing, unless the 
behavior would be ambiguous.

4) Purity & side effects
- Functions must be pure: no global state, no IO, no randomness without an explicit seed.
- If randomness is needed, accept an optional numeric seed and use a local PRNG instance so results are reproducible 
  for the same inputs. (See example below.)

5) Data handling rules
When reading values from row:
- Coercion: Convert non-string primitives to strings with String(value).
- Trimming: value.replace(/\s+/g, " ").trim() to normalize whitespace.
- Null/undefined: Treat as missing; skip in joins/concats.
- Ordering: If order matters, rely on columns order unless the function explicitly randomizes/shuffles.
- Deduplication: Only if the function’s semantics require it; otherwise preserve duplicates.

6) Errors & edge cases
- columns contains names not present in row → treat as missing.
- Requested output count <= 0 → return [].
- Excessive output count (e.g., > 1000) → cap at 1000 unless otherwise specified by the function.

7) Performance constraints
- Assume high throughput: avoid O(n²) string concatenations in loops; build with arrays and join.
- Precompile regexes outside tight loops when possible.
- Do not allocate large temporary arrays unless necessary.

8) Documentation (JSDoc required)
Every exposed function MUST include a complete JSDoc:
- Summary line describing the purpose.
- @param for each parameter in order.
- Use Object.<string, *> for row.
- Use string[] for columns.
- Optional params use square brackets and note defaults in the description.
- @returns {string[]} with a clear description.
- Document error behavior and edge cases.

Template:
/**
* <One-line summary>.
*
* @param {Object.<string, *>} row - A single record (values typically strings).
* @param {string[]} columns - Ordered list of column names available in `row`.
* @param {string} [separator=", "] - <Describe optional param and default>.
* @param {number} [outputCount=1] - <Describe optional param and default>.
* @returns {string[]} <Describe the output semantics>.
* @throws {Error} If required parameters are missing or of wrong type.
*/
  function example(row, columns, separator, outputCount) { ... }

9) Testing guidelines
Provide a additional tiny function for manual testing without parameters and its name must start with "test":
function testExample() {
  var row = { house_number: " 12 ", street: "Main St", city: "Berlin", country: "" };
  var columns = ["house_number","street","city","country"];
  const resalt = testRandomPermutationJoin(row, columns, " ", 10);    
  return JSON.stringify(resalt);
}
Do not rely on external frameworks. Simple returning string is sufficient for smoke tests.

10) Packaging & delivery
- One .js file with "use strict";.
- No transpilation required; do not include bundlers or node_modules.
- Each file should contain only the exposed functions and helper testing code local to those functions.