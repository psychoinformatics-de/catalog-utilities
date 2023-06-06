# catalog-utilities


How to install and run:

```
git clone https://github.com/psychoinformatics-de/catalog-utilities.git
cd catalog-utilities

chmod -R u+rwx code/*

python code/create_catalog_metadata.py -m data/dataset_metadata.tsv -t dataset
```

Output in: `data/dataset_metadata.jsonl`