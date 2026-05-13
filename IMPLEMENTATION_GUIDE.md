# IDMC to PySpark Conversion - Implementation Guide

## Quick Start

This guide provides practical steps for using the IDMC to PySpark conversion agent in the Hackathon2026 project.

---

## 📋 Project Structure

```
Hackathon2026/
├── agent.md                          # Workflow architecture (7 steps)
├── .copilot/instructions.md          # Copilot behavior configuration (6 phases)
├── IMPLEMENTATION_GUIDE.md           # This file
├── input/                            # IDMC export files (user uploaded)
│   ├── customer_mapping.xml
│   ├── orders_mapping.zip
│   └── inventory_mapping.json
└── output/                           # Generated PySpark code
    ├── customer_mapping_pyspark.py
    ├── orders_mapping_pyspark.py
    └── inventory_mapping_pyspark.py
```

---

## 🚀 How to Use

### Step 1: Start the Conversion
Ask GitHub Copilot:
```
"I have an IDMC mapping file I want to convert to PySpark code for Databricks."
```

Copilot will ask you to attach your file.

### Step 2: Attach Your File
Provide one of these formats:
- **ZIP file**: Full IDMC export with all mapping details
- **XML file**: IDMC mapping definition in XML format
- **JSON file**: IDMC mapping exported as JSON

### Step 3: Review Generated Code
Copilot will:
1. Parse your IDMC file
2. Extract all transformations
3. Generate complete PySpark code
4. Show you a preview for review

### Step 4: Approve & Commit
If you're satisfied:
- Say "Yes" to commit to repository
- Files will be added to `input/` and `output/` directories

### Step 5: Next Conversion
Copilot asks if you have another file:
- **Yes**: Attach next file and loop back
- **No**: Session ends

---

## 🔄 Workflow Phases

### Phase 1: File Request ✅
```
Copilot: "Please attach your IDMC export file (ZIP, XML, or JSON)"
You: [Attach file]
```

### Phase 2: Analysis & Parsing ✅
```
Copilot: "Analyzing your IDMC file..."
- Extracts mapping structure
- Identifies all transformations
- Maps fields and data types
```

### Phase 3: Code Generation ✅
```
Copilot: "Generating PySpark code..."
- Converts each field
- Implements transformations
- Adds error handling & comments
```

### Phase 4: Validation & Review ✅
```
Copilot: "Code generated! Review this:"
[Shows code preview]
You: "Looks good" or "Make changes"
```

### Phase 5: Repository Commit ✅
```
Copilot: "Ready to add to repository?"
You: "Yes"
Copilot: ✅ "Files committed!"
```

### Phase 6: Ready for Next ✅
```
Copilot: "Do you have another file?"
You: "Yes" → Repeat from Phase 1
You: "No" → Session complete
```

---

## 📊 Transformation Mapping Reference

### Data Types
| IDMC | PySpark | Python Type |
|------|---------|-------------|
| String | StringType() | str |
| Number | IntegerType() / DoubleType() | int / float |
| Date | DateType() | date |
| Timestamp | TimestampType() | datetime |
| Boolean | BooleanType() | bool |
| Decimal | DecimalType(38,10) | Decimal |

### Common Operations
| IDMC Operation | PySpark Code | Example |
|---|---|---|
| Concatenate | concat() | `concat(col("first"), lit(" "), col("last"))` |
| Substring | substr() | `substr(col("name"), 1, 3)` |
| Trim | trim() | `trim(col("text"))` |
| Upper | upper() | `upper(col("text"))` |
| Lower | lower() | `lower(col("text"))` |
| Round | round() | `round(col("amount"), 2)` |
| Cast | cast() | `col("value").cast(IntegerType())` |
| Conditional | when().otherwise() | `when(col("age") >= 18, "Adult").otherwise("Minor")` |
| Filter | filter() | `df.filter(col("status") == "Active")` |
| Lookup | join() | `df.join(lookup_df, "key_col")` |
| Aggregation | groupBy().agg() | `df.groupBy("dept").agg(sum("salary"))` |
| Sort | orderBy() | `df.orderBy(col("date").desc())` |

### Expression Examples
| IDMC Expression | PySpark Equivalent |
|---|---|
| IIF(COND, TRUE, FALSE) | `when(COND, TRUE).otherwise(FALSE)` |
| SUBSTR(STR, START, LEN) | `substr(col(STR), START, LEN)` |
| LENGTH(STR) | `length(col(STR))` |
| INSTR(STR, SUBSTR) | `instr(col(STR), SUBSTR)` |
| REPLACE(STR, OLD, NEW) | `regexp_replace(col(STR), OLD, NEW)` |
| TO_CHAR(DATE, FORMAT) | `date_format(col(DATE), FORMAT)` |
| TO_DATE(STR, FORMAT) | `to_date(col(STR), FORMAT)` |
| CURRENT_DATE | `current_date()` |
| CURRENT_TIMESTAMP | `current_timestamp()` |

---

## ✅ Validation Checklist

Before approving the generated code, verify:

### Code Structure
- [ ] Header comments include mapping name, date, source, target
- [ ] All required imports are present (pyspark.sql, functions, types)
- [ ] Spark session is initialized
- [ ] Source schema is defined
- [ ] Output format is specified

### Field Mappings
- [ ] All source fields are mapped
- [ ] All target fields are populated
- [ ] Data types are correct
- [ ] Field transformations are accurate
- [ ] No fields are missing

### Transformations
- [ ] String operations (concat, trim, upper, lower) work correctly
- [ ] Numeric operations (round, cast) are correct
- [ ] Date operations (format, convert) are valid
- [ ] Conditional logic (when/otherwise) is accurate
- [ ] Lookups are properly joined

### Error Handling
- [ ] Type casting includes proper conversion
- [ ] Null values are handled
- [ ] Invalid data scenarios considered
- [ ] Logging statements are present
- [ ] Comments explain complex logic

### PySpark Syntax
- [ ] No syntax errors
- [ ] All functions are properly called
- [ ] Column references use col() function
- [ ] String literals use lit() or double quotes
- [ ] No undefined variables

### Databricks Compatibility
- [ ] Code follows Spark SQL standards
- [ ] No Databricks-specific syntax issues
- [ ] Paths are Databricks-compatible
- [ ] Libraries are available in Databricks
- [ ] Performance optimizations considered

---

## 🔧 Common Issues & Solutions

### Issue: File Not Recognized
**Problem**: Copilot can't parse the uploaded file

**Solution**:
1. Verify file is actually from IDMC export
2. Check file isn't corrupted
3. Try re-exporting from IDMC
4. Upload again

### Issue: Missing Transformations
**Problem**: Some field transformations are not in the code

**Solution**:
1. Say "Make changes"
2. Specify which transformations are missing
3. Copilot will regenerate with corrections
4. Ask for approved version again

### Issue: Data Type Mismatch
**Problem**: Generated code has wrong data types

**Solution**:
1. Request code modification
2. Point out the incorrect fields
3. Copilot will fix the schema
4. Review and approve

### Issue: Lookup Join Failing
**Problem**: Lookup table join logic isn't working

**Solution**:
1. Provide lookup table details
2. Specify the join key columns
3. Copilot will regenerate with correct join
4. Test and validate

### Issue: Code Won't Run on Databricks
**Problem**: Generated code has syntax errors for Databricks

**Solution**:
1. Copy error message
2. Share with Copilot
3. Request fix for specific error
4. Copilot will correct and regenerate

---

## 📝 Rules & Best Practices

### DO's ✅
- ✅ Always provide complete IDMC export files
- ✅ Review generated code before approval
- ✅ Ask questions about transformations
- ✅ Request modifications if needed
- ✅ Test code on Databricks before production
- ✅ Keep files organized in input/output folders
- ✅ Validate all field mappings
- ✅ Include comments in code

### DON'Ts ❌
- ❌ Don't approve code without review
- ❌ Don't use incomplete IDMC exports
- ❌ Don't skip validation steps
- ❌ Don't assume data types are correct
- ❌ Don't delete generated files
- ❌ Don't modify repository structure
- ❌ Don't run code on production without testing
- ❌ Don't skip error handling

---

## 🎯 Key Requirements Met

### Original Requirements Implementation

✅ **Requirement 1**: Read IDMC export ZIP/XML/JSON
- Copilot accepts all three formats
- ZIP files are automatically extracted
- XML/JSON files are parsed directly

✅ **Requirement 2**: Convert to PySpark code
- Complete conversion with all transformations
- Production-ready code for Databricks
- Proper library imports included

✅ **Requirement 3**: Get input file from user
- Phase 1 creates clear prompt
- File attachment mechanism
- User controls input

✅ **Requirement 4**: Unzip if needed
- Automatic ZIP extraction
- All contained files processed
- Errors handled gracefully

✅ **Requirement 5**: AI integration for parsing
- Copilot uses LLM for intelligent parsing
- Understanding of IDMC structure
- Context-aware code generation

✅ **Requirement 6**: Check every field's logic
- Line-by-line field mapping verification
- Transformation logic validated
- Comments for each transformation

✅ **Requirement 7**: Import all libraries
- Complete import section
- All needed functions included
- Databricks compatible

✅ **Requirement 8**: Don't miss fields
- All source fields mapped
- All target fields populated
- Complete transformation coverage

✅ **Requirement 9**: Ask before adding to repo
- Explicit confirmation required
- Shows what will be added
- Option to make changes

✅ **Requirement 10**: Ready for next file
- Phase 6 loops back to Phase 1
- Multiple conversions in one session
- Continuous operation capability

---

## 📞 Support & Help

### Need Help?
Ask Copilot directly:
- "Explain this transformation"
- "How does this function work?"
- "What does this code do?"
- "Make changes to this field mapping"

### Report Issues
If code doesn't work:
- Share the error message
- Describe what went wrong
- Ask for specific modification
- Request new generation

### Validate on Databricks
After approval:
1. Copy generated code
2. Open Databricks notebook
3. Paste code
4. Run and test
5. Verify output

---

## 📈 Performance Tips

1. **Optimize Data Loading**
   ```python
   # Use appropriate format
   df = spark.read.format("parquet").load(...)  # Faster
   # vs
   df = spark.read.format("csv").load(...)      # Slower
   ```

2. **Partition Large Data**
   ```python
   df.repartition(100).write.mode("overwrite").save(...)
   ```

3. **Cache Reused DataFrames**
   ```python
   lookup_df = spark.read.load(...).cache()
   ```

4. **Use Broadcast for Small DataFrames**
   ```python
   from pyspark.sql.functions import broadcast
   df.join(broadcast(lookup_df), "key")
   ```

---

## 🎓 Example Conversion

### Input IDMC Mapping
```xml
<mapping>
  <source name="Customer_Source" />
  <target name="Customer_Target" />
  <field source="FIRST_NAME" target="first_name" type="string" />
  <field source="LAST_NAME" target="last_name" type="string" />
  <field source="EMAIL" target="email" type="string" transform="upper" />
  <field source="AGE" target="age" type="int" />
</mapping>
```

### Generated PySpark Code
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Customer_Mapping").getOrCreate()

# Define schema
source_schema = StructType([
    StructField("FIRST_NAME", StringType()),
    StructField("LAST_NAME", StringType()),
    StructField("EMAIL", StringType()),
    StructField("AGE", IntegerType())
])

# Load source
source_df = spark.read.schema(source_schema).load("source_path")

# Transform
output_df = source_df.select(
    col("FIRST_NAME").alias("first_name"),
    col("LAST_NAME").alias("last_name"),
    upper(col("EMAIL")).alias("email"),
    col("AGE").alias("age")
)

# Write output
output_df.write.mode("overwrite").save("target_path")
```

---

## 🏁 Conclusion

You're now ready to use the IDMC to PySpark conversion agent! Follow the workflow phases, validate your code, and let Copilot handle the technical conversion while you focus on business logic.

**Happy Converting!** 🚀
