# Hierarchy of tables
See [`tables.yaml`](tables.yaml#L3) for table permissions and description. Name of the database can be set under the [`dbname`](tables.yaml#L1) field and the deafult name is `hgcdb`. [`Users`](tables.yaml#L154) are defined at the end.

## Tables in this database
- module_info
  - module_assembly
  - module_inspect
  - module_iv_test
  - module_pedestal_test
  - module_pedestal_plots
  - module_qc_summary
  - proto_assembly
    - proto_inspect
    - baseplate
      - bp_inspect
    - sensor
  - hexaboard
    - hxb_inspect
    - hxb_pedestal_test

## Note to developers
The comments in the CSV files in `dbase_info/postgres_tables` do not accept single or double quotes but the backtick \` symbol is okay. For example, \`A` is allowed but \'A' and \"A" are not allowed in the comments. Each row should have four commas. And if it doesn't render properly on GitHub as a table, that is an indication that we screwed up the entries in a row.
  
## Table hierarchy generated from pgAdmin4
To view chart in pgAdmin4, right-click on the name of the database and select 'ERD for Database'.


![Table hierarchy generated from pgAdmin4](../documentation/images/db_at_a_glance.png?raw=true)


