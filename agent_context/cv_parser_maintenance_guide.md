# SAP HANA Calculation View Parser - Maintenance Guide

**For Claude Code Instances**

This guide provides complete instructions for maintaining and extending the calculation view XML ↔ Pydantic parser system with byte-for-byte accuracy.

## System Overview

### Goal
Parse SAP HANA calculation view XML files to Pydantic models and back to XML with **byte-for-byte accuracy**.

### Flow
```
XML → dict → Pydantic model → dict → XML (byte-identical)
```

### Key Files
- `cv/cv_types/models.py` - Pydantic models (520+ lines)
- `cv/cv_types/enums.py` - All enum types (160+ lines)
- `cv/deserialize.py` - XML → dict parser with Pydantic validation
- `cv/serialize.py` - dict → XML serializer
- `test_all.py` - Comprehensive test script for all views

### Directory Structure
```
inputs/
  cv/                      # Original views (CRLF line endings)
  cv_remediated/           # Remediated views (LF line endings)
outputs/
  deserializer/            # .pickle files (dict representations)
  serializer/              # Regenerated .calculationview files
```

---

## Iterative Testing & Fixing Workflow

### Step 1: Test New Calculation Views

**Command:**
```bash
python3 test_all.py
```

Or test individual views:
```bash
# Test directory 1 (original), view 4
echo -e "1\n4" | python3 cv/deserialize.py

# Test directory 2 (remediated), view 1
echo -e "2\n1" | python3 cv/deserialize.py
```

**Expected Output:**
```
✓ Pydantic validation PASSED
```

**Failure Output:**
```
✗ Pydantic validation FAILED
Error: N validation errors for CalculationScenario
```

---

### Step 2: Diagnose Validation Errors

When validation fails, the output shows specific error patterns. Common error types:

#### Error Type 1: Missing Enum Value
```
Error: Input should be 'X', 'Y', 'Z' [type=enum, input_value='NewValue', input_type=str]
```

**Fix:** Add missing enum value to `cv/cv_types/enums.py`

**Example:**
```python
class XsiType(str, Enum):
    """xsi:type attribute values"""
    AGGREGATION_VIEW = "Calculation:AggregationView"
    JOIN_VIEW = "Calculation:JoinView"
    NEW_VIEW_TYPE = "Calculation:NewViewType"  # ADD THIS
```

#### Error Type 2: Container Structure Mismatch
```
Error: Input should be a valid list [type=list_type, input_value={'someTag': [...]}, input_type=dict]
```

This means the parser created `{'someTag': [...]}` but the model expects a direct list.

**Fix:** Create a container class in `cv/cv_types/models.py`

**Pattern:**
```python
# Before (WRONG):
class ParentModel(BaseModel):
    children: List[ChildModel]  # Expects direct list

# After (CORRECT):
class ChildrenContainer(BaseModel):
    """Container for child elements"""
    child: List[ChildModel] = Field(default_factory=list)

    @field_validator('child', mode='before')
    @classmethod
    def normalize_to_list(cls, v):
        """Convert single item to list"""
        if isinstance(v, dict):
            return [v]
        return v if v is not None else []

class ParentModel(BaseModel):
    children: Optional[ChildrenContainer] = None
```

**Why?** The XML parser creates `{'child': [item1, item2]}` for XML like:
```xml
<parent>
  <children>
    <child>...</child>
    <child>...</child>
  </children>
</parent>
```

#### Error Type 3: Namespace Attribute Issues
```
Error: Field 'xsi:type' required [type=missing, input_value={'xsi_type': '...'}]
```

**Fix:** Remove `Field(alias="xsi:type")` - the deserializer converts colons to underscores

**Pattern:**
```python
# Before (WRONG):
class MyModel(BaseModel):
    xsi_type: XsiType = Field(alias="xsi:type")

# After (CORRECT):
class MyModel(BaseModel):
    xsi_type: XsiType  # Deserializer already converts xsi:type → xsi_type
```

#### Error Type 4: Missing Optional Fields
```
Error: Field required [type=missing, input_value={...}]
```

**Fix:** Make field optional

**Pattern:**
```python
# Before (WRONG):
class MyModel(BaseModel):
    requiredField: str

# After (CORRECT):
class MyModel(BaseModel):
    requiredField: Optional[str] = None
```

#### Error Type 5: New Calculation View Type
```
Error: Input should be union[AggregationView, JoinView, ...] but got 'NewViewType'
```

**Fix:** Add new view type to `cv/cv_types/models.py`

**Pattern:**
```python
# 1. Create the model
class NewViewType(CalculationViewBase):
    """New view type description"""
    # Add any specific fields for this type
    specificField: Optional[str] = None

# 2. Add to union
CalculationView = Union[
    AggregationView,
    JoinView,
    ProjectionView,
    UnionView,
    RankView,
    NewViewType  # ADD HERE
]
```

---

### Step 3: Apply Fixes Systematically

**IMPORTANT:** Fix errors in batches by type, not individually.

**Recommended Order:**
1. Fix missing enum values (~30 sec per fix)
2. Fix namespace attribute issues (~1 min per file)
3. Create container classes (~2 min per class)
4. Fix optional fields (~30 sec per field)
5. Add new model types (~5 min per type)

**After each batch of fixes:**
```bash
echo -e "1\n4" | python3 cv/deserialize.py 2>&1 | grep "validation errors"
```

Watch the error count decrease: `1045 → 745 → 245 → 0`

---

### Step 4: Verify Byte-for-Byte Accuracy

Once Pydantic validation passes, test serialization:

```bash
# Run deserializer (creates pickle)
echo -e "1\n4" | python3 cv/deserialize.py

# Run serializer (recreates XML and compares)
echo -e "1\n4" | python3 cv/serialize.py
```

**Success:**
```
✓ PERFECT MATCH - Files are byte-for-byte identical!
```

**Failure:**
```
✗ Files differ
  Original size:   72794 bytes
  Serialized size: 72800 bytes
  Difference:      +6 bytes

  First difference at byte 1234:
    Original:   '>' (0x3e)
    Serialized: '&' (0x26)
```

---

## Common Serialization Issues & Fixes

### Issue 1: Over-Escaping Characters

**Symptom:** Serialized file is larger, shows `&gt;` instead of `>`

**Root Cause:** Serializer is escaping characters that don't need escaping in text content

**Check `cv/serialize.py` escape_xml():**
```python
def escape_xml(text: str, for_attribute: bool = False) -> str:
    """Only escape what's necessary"""
    text = text.replace('&', '&amp;')   # Always escape
    text = text.replace('<', '&lt;')    # Always escape
    text = text.replace('"', '&quot;')  # Always escape
    text = text.replace('\r', '&#xD;')  # Escape CR as hex
    # Don't escape > or ' - not needed in text content
    return text
```

### Issue 2: Line Ending Mismatch

**Symptom:** First difference at byte 38 (right after XML declaration)

**Root Cause:** File uses LF but serializer outputs CRLF (or vice versa)

**How It Works:**
- Deserializer auto-detects line endings and stores in `_line_ending` metadata
- Serializer uses detected style
- Original files use CRLF (`\r\n`)
- Remediated files use LF (`\n`)

**If broken:** Check `cv/deserialize.py` line 108:
```python
# Detect line ending style (CRLF vs LF)
line_ending = '\r\n' if b'\r\n' in first_bytes else '\n'
```

### Issue 3: Text Content Formatting

**Symptom:** Text appears on separate lines instead of inline

**Root Cause:** Serializer treats text-only elements as having children

**Check `cv/serialize.py` dict_to_xml_string():**
```python
# Special case: if only text content (no children), keep it inline
if text_content and not children:
    escaped_text = escape_xml(text_content)
    # Normalize line endings in text to match file format
    escaped_text = escaped_text.replace('\r\n', '\n').replace('\n', line_ending)
    return f"{indent}<{' '.join(tag_parts)}>{escaped_text}</{tag}>"
```

### Issue 4: Missing Trailing Whitespace

**Symptom:** Serialized file is slightly shorter

**Root Cause:** Deserializer strips whitespace it shouldn't

**Check `cv/deserialize.py` line 136:**
```python
# Get text content - DON'T strip() to preserve exact content including trailing &#xD;
text = elem.text if elem.text else ''

# BUT: only store if non-empty after strip (to ignore pretty-print whitespace)
# yet preserve the original unstripped text
if text and text.strip():
    result['_text'] = text  # Store ORIGINAL unstripped text
```

### Issue 5: Attribute Order Differences

**Symptom:** Attributes appear in different order

**Root Cause:** Dictionary iteration order or xmlns ordering

**Fix in `cv/serialize.py`:**
```python
# xmlns declarations must come first, in specific order
xmlns_order = ['xmlns:xsi', 'xmlns:AccessControl', 'xmlns:Calculation', 'xmlns:Variable']
for xmlns_key in xmlns_order:
    if xmlns_key in data:
        root_attrs.append((xmlns_key, data[xmlns_key]))
```

---

## Critical Principles

### 1. Parser Structure Consistency

**The XML parser creates a CONSISTENT PATTERN:**

Every XML element with multiple children of the same tag becomes:
```python
{'childTag': [item1, item2, ...]}
```

For single children, it can be either:
```python
{'childTag': item}  # Single occurrence
{'childTag': [item]}  # Could be multiple but only one present
```

**Pydantic models MUST match this exact structure using container classes.**

### 2. Namespace Handling

The deserializer converts namespace prefixes:
- `xsi:type` → `xsi_type`
- `AccessControl:something` → `AccessControl_something`
- `xmlns:xsi` → `xmlns:xsi` (preserved as-is)

The serializer MUST reverse this conversion.

### 3. Text Content Preservation

**NEVER strip() text content** - it may contain important trailing characters like `\r` that become `&#xD;` in XML.

Only check `.strip()` to determine if text exists, but always store the original.

### 4. Field Validators for Single-Item Lists

**ALWAYS add this validator to container classes:**
```python
@field_validator('fieldName', mode='before')
@classmethod
def normalize_to_list(cls, v):
    """Convert single item to list"""
    if isinstance(v, dict):
        return [v]
    return v if v is not None else []
```

This handles XML with only one occurrence of a repeatable element.

### 5. Line Ending Preservation

**Never hardcode `\r\n` or `\n`** - always use the detected `line_ending` from metadata.

---

## Testing Strategy

### Comprehensive Test

Run all 21 views:
```bash
python3 test_all.py
```

Expected output:
```
================================================================================
Directory 1: Original Calculation Views (12 views)
================================================================================
 1. CV_FSA_ASST_PSTNG_DTL_A02.calculationview          ✓ PASSED
 2. CV_FSA_TRANSIT_DTL_BSEG.calculationview            ✓ PASSED
 ... (all pass)

================================================================================
Directory 2: Remediated Calculation Views (9 views)
================================================================================
 1. CV_FSA_ASST_PSTNG_DTL_A02_ECC.calculationview      ✓ PASSED
 ... (all pass)
```

### Individual View Testing

```bash
# Test specific view
echo -e "1\n4" | python3 cv/deserialize.py

# If validation passes, test serialization
echo -e "1\n4" | python3 cv/serialize.py
```

### Quick Validation Check

```bash
# Just see error count
echo -e "1\n4" | python3 cv/deserialize.py 2>&1 | grep "validation errors"
```

---

## Debugging Tips

### 1. Inspect Pickle Contents

```python
import pickle
with open('outputs/deserializer/CV_NAME.pickle', 'rb') as f:
    data = pickle.load(f)

# Check structure
print('Keys:', list(data.keys())[:10])

# Check nested structure
calc_views = data.get('calculationViews', {})
if 'calculationView' in calc_views:
    first_view = calc_views['calculationView'][0]
    print('First calc view keys:', list(first_view.keys()))
```

### 2. Find Specific Errors

```bash
# See all unique error types
echo -e "1\n4" | python3 cv/deserialize.py 2>&1 | grep "Input should" | sort -u

# Count errors by type
echo -e "1\n4" | python3 cv/deserialize.py 2>&1 | grep "Input should" | sort | uniq -c
```

### 3. Compare XML Files

```bash
# See first difference location
diff inputs/cv/FILE.calculationview outputs/serializer/FILE.calculationview | head -20

# Binary comparison
cmp -l inputs/cv/FILE.calculationview outputs/serializer/FILE.calculationview | head -10
```

### 4. Check Specific XML Structure

```bash
# View specific lines
sed -n '100,110p' inputs/cv/FILE.calculationview

# Search for element
grep -n "elementName" inputs/cv/FILE.calculationview
```

---

## Quick Reference: Model Patterns

### Empty Element
```python
class Origin(BaseModel):
    """Empty element"""
    pass
```

### Simple Element with Attributes
```python
class Metadata(BaseModel):
    """Element with attributes only"""
    activatedAt: Optional[str] = None
    changedAt: Optional[str] = None
```

### Element with Text Content
```python
class DefaultSchema(BaseModel):
    """Element with text child"""
    schemaName: str  # Maps to <defaultSchema schemaName="value"/>
```

### Container Class Pattern
```python
class ItemsContainer(BaseModel):
    """Container for repeated elements"""
    item: List[ItemType] = Field(default_factory=list)

    @field_validator('item', mode='before')
    @classmethod
    def normalize_to_list(cls, v):
        if isinstance(v, dict):
            return [v]
        return v if v is not None else []
```

### Polymorphic Elements (Discriminated Union)
```python
class BaseView(BaseModel):
    xsi_type: XsiType  # Discriminator
    id: str

class AggregationView(BaseView):
    pass

class JoinView(BaseView):
    cardinality: Optional[Cardinality] = None

# Union
CalculationView = Union[AggregationView, JoinView, ...]
```

### Element with Namespace Attributes
```python
class VariableMapping(BaseModel):
    xsi_type: XsiType  # NOT Field(alias="xsi:type")
    dataSource: str
```

---

## Success Criteria

✓ **All validation errors resolved**
```
✓ Pydantic validation PASSED
```

✓ **Byte-for-byte serialization accuracy**
```
✓ PERFECT MATCH - Files are byte-for-byte identical!
```

✓ **All 21 views passing**
- Directory 1 (original): 12/12 views
- Directory 2 (remediated): 9/9 views

---

## When to Update This Guide

Add examples if you encounter:
- New error patterns not covered above
- New calculation view types
- Special XML structures that require unique handling
- Edge cases in serialization

---

## File Locations Reference

```
cv/
├── cv_types/
│   ├── __init__.py
│   ├── enums.py              # All enum types
│   └── models.py             # Pydantic models
├── deserialize.py            # XML → dict parser
└── serialize.py              # dict → XML serializer

inputs/
├── cv/                       # Original views (CRLF)
└── cv_remediated/            # Remediated views (LF)

outputs/
├── deserializer/             # .pickle files
└── serializer/               # Regenerated .calculationview files

agents/
└── cv_parser_maintenance_guide.md  # This file

test_all.py                   # Comprehensive test script
```

---

## Emergency Recovery

If you completely break the system:

1. **Restore models from backup:**
   ```bash
   cp cv/cv_types/models_backup.py cv/cv_types/models.py
   ```

2. **Check git history:**
   ```bash
   git log --oneline cv/cv_types/models.py
   git diff HEAD~1 cv/cv_types/models.py
   ```

3. **Start with simplest view:**
   ```bash
   # Find smallest file
   ls -lh inputs/cv/*.calculationview | sort -k5 -h | head -1
   ```

4. **Rebuild incrementally:**
   - Test smallest view
   - Fix errors
   - Test next smallest view
   - Repeat

---

**Last Updated:** After achieving 21/21 views with byte-for-byte accuracy
**System Status:** Production-ready ✓
