
#!/usr/bin/env python3
"""

"""
from datetime import datetime
import json
import logging
from pathlib import Path
import subprocess

lgr = logging.getLogger('catalog-utilities')

# The dataset schema defines recognized fields contained in
# incoming dataset metadata, and their properties:
# - type: could be a single text field, or a list of text fields
# - required: whether the field is required for the purpose of
#   creating a catalog entry
# - case: could be 'single' or 'multiple', indicating whether the
#   field can only be supplied once or multiple times in the metadata file
# - columns: the headings of all columns, in order, for the fields
#   where type == 'list'
# Currently, the 'type' and 'case' properties are not used
dataset_schema = {
    'identifier': {
        'type': 'text',
        'required': True,
        'case': 'single',
    },
    'version': {
        'type': 'text',
        'required': False,
        'case': 'single',
    },
    'name': {
        'type': 'text',
        'required': True,
        'case': 'single',
    },
    'description': {
        'type': 'text',
        'required': False,
        'case': 'single',
    },
    'author': {
        'type': 'list',
        'required': False,
        'case': 'multiple',
        'columns': ['full_name', 'orcid', 'email', 'affiliations'],
    },
    'publication': {
        'type': 'list',
        'required': False,
        'case': 'multiple',
        'columns': ['doi', 'citation'],
    },
    'keywords': {
        'type': 'list',
        'required': False,
        'case': 'single',
        # how to define the situation where all columns of the list have the same definition
    },
    'property': {
        'type': 'list',
        'required': False,
        'case': 'multiple',
        'columns': ['name','value'],
    },
    'sfb1451': {
        'type': 'list',
        'required': False,
        'case': 'multiple',
        'columns': ['name','value'],
    },
}

# This defines mapping of field names from incoming metadata
# to corresponding catalog fields
dataset_catalog_mapping = {
    'identifier': 'dataset_id',
    'version': 'dataset_version',
    'name': 'name',
    'description': 'description',
    'author': 'authors',
    'publication': 'publications',
    'keywords': 'keywords',
    'property': 'top_display',
    'sfb1451': 'additional_display'
}
# and the inverse
catalog_dataset_mapping = {v: k for k, v in dataset_catalog_mapping.items()}

file_schema = {
}


def main(metadata: str, metadata_type: str):
    """Main function called with command line arguments
    
    Checks a few basic constraints and then calls the 
    relevant metadata transformation method (dataset | file)"""
    metadata_path = Path(metadata).resolve()
    # some basic checks
    if not metadata_path.exists():
        raise ValueError(f'No file available at {metadata_path}')
    if metadata_path.suffix != '.tsv' :
        raise ValueError(f'Cannot operate on a non-TSV file: {metadata_path}')
    # output to be saved at same path as input, only with a different extension
    output_path = metadata_path.with_suffix('.jsonl')
    # call the appropriate method to transform metadata
    if metadata_type == 'dataset':
        transform_dataset_metadata(
            input_path=metadata_path,
            output_path=output_path,
        )
    else:
        transform_file_metadata(
            input_path=metadata_path,
            output_path=output_path,
        )


def transform_dataset_metadata(input_path, output_path):
    """Reads and transforms dataset metadata from TSV format to JSON"""
    metadata = {}
    i = 0
    with open(input_path) as file:
        for line in file:
            i+=1
            l = line.rstrip().split('\t')
            try:
                key = l[0]
                # get item schema and handle non-recognized keys
                item_schema = dataset_schema.get(key, None)
                if item_schema is None:
                    # skip for now. TODO: different handling?
                    lgr.info(msg=f'non-recognized field encountered in line {i}: {key}')
                    continue
                catalog_key, value = parse_dataset_columns(key, l[1:], item_schema)
                add_metadata_item(catalog_key, value, metadata)
            except Exception as e:
                lgr.error(msg=f'Error encountered on line {i}', exc_info=e)
    # Now we have the metadata in an almost catalog-valid format, but still to do:
    # - Some fields still require some wrangling to be catalog-valid
    # - Some non-content-specific fields still need to be added
    metadata = map_to_catalog(metadata)
    print(json.dumps(metadata))
    # Finally write json line to file
    with open(output_path, 'w') as outfile:
        json.dump(metadata, outfile)


def parse_dataset_columns(key: str, value: list, item_schema: dict):
    # Here the type of metadata field and associated columns are handled
    # based on the amount of columns, i.e. based on supplied data which
    # could be wrong. We might consider rather handling it based on the
    # definition encoded in the dataset_schema dict.
    if len(value) > 1:
        # handle the case where the field has values in multiple columns
        catalog_key = dataset_catalog_mapping[key]
        columns = item_schema.get('columns', None)
        if columns is None:
            # this is interpreted as all columns having the same
            # definition e.g. keywords. We just return the same list.
            return catalog_key, value
        else:
            # Map elements of the list onto column names from the schema
            # but first make sure that list lengths are equal
            if len(columns) > len(value):
                columns = columns[:len(value)]
            new_value = {k: v for k, v in zip(columns, value)}
            return catalog_key, new_value
    else:
        # Handle the simple case: direct mapping
        return dataset_catalog_mapping[key], value[0]


def add_metadata_item(key, value, metadata):
    """"""
    # If the field has already been supplied, the default
    # is to assume that it is intentionally supplied multiple
    # times, i.e. that it will eventually be an element in a list
    # If this is undesireable, the 'case' property from the schema
    # could be incorporated
    # print(f'key: {key}; value: {value}; metadata {metadata}')
    if key in metadata.keys():
        if not isinstance(metadata[key], list):
            # first make sure that the existing value is a list
            metadata[key] = [metadata[key]]
        metadata[key].append(value)
    else:
        metadata[key] = value


def map_to_catalog(metadata):
    """"""
    # Get basic valid metadata item
    meta_item = new_dataset_meta_item(
        ds_id=get_dataset_id(metadata),
        ds_version=get_dataset_version(metadata),
        ds_name=metadata.get('name', ''),
        ds_description=metadata.get('description', ''),
    )
    # map and add remaining fields to meta_item
    for key in metadata.keys():
        if key in meta_item.keys():
            continue
        # some fields require wrangling:
        # - authors
        # - additional_display
        # - publications
        # other fields are mapped directly from their current value
        if key == 'authors':
            if key not in meta_item.keys():
                meta_item[key] = []
            for author in metadata[key]:
                meta_item[key].append(
                    get_author(author)
                )
        elif key == 'additional_display':
            if key not in meta_item.keys():
                meta_item[key] = []
            meta_item[key].append(
                get_additional_display(metadata[key])
            )
        elif key == 'publications':
            if key not in meta_item.keys():
                meta_item[key] = []
            for pub in metadata[key]:
                meta_item[key].append(
                    get_publication(pub)
                )
        else:
            meta_item[key] = metadata[key]

    return meta_item


def transform_file_metadata(input_path, output_path):
    """Reads and transforms file metadata from TSV format to JSON"""
    raise NotImplementedError


def get_dataset_id(input):
    """"""
    # TODO: ideally determine the dataset id deterministically
    # using incoming identifier, name, project, etc
    # For now, just return same
    return input['dataset_id']


def get_dataset_version(input):
    """"""
    # Version is required for catalog, but not for incoming metadata
    # TODO: what to do here?
    # For now, just return 'latest' if not provided
    v = input.get('dataset_version', None)
    return str(v) if v is not None else 'latest'


def get_author(author):
    full_name = author.get('full_name', None)
    email = author.get('email', None)
    orcid = author.get('orcid', None)
    identifiers = [{
        'type': 'ORCID',
        'identifier': orcid
    }] if orcid is not None else []
    # TODO: where to put 'affiliations', which is part of incoming metadata
    return {
      'name': full_name if full_name is not None else '',
      'givenName': '',
      'familyName': '',
      'email': email if email is not None else '',
      'honorificSuffix': '',
      'identifiers': identifiers
    }


def get_additional_display(display):
    content = {}
    for d in display:
        content[d['name']] = d['value']
    return {
      'name': catalog_dataset_mapping['additional_display'],
      'content': content,
    }

def get_publication(publication):
    # catalog publications expect: title, doi, authors
    # incoming metadata provides: doi, citation
    return {
        'type': '',
        'title': publication.get('citation', ''),
        'doi': publication.get('doi', ''),
        'datePublished': '',
        'publicationOutlet': '',
        'authors': []
    }


def get_gitconfig(conf_name):
    """Get current user's git config to append to metadata item for catalog"""
    result = (
        subprocess.run(['git', 'config', conf_name], capture_output=True)
        .stdout.decode()
        .rstrip()
    )
    return result


def get_metadata_source():
    """Create metadata_sources dict required by catalog schema"""
    source = {
        'key_source_map': {},
        'sources': [
            {
                'source_name': 'manual_to_automated_addition',
                'source_version': '0.1.0',
                'source_time': datetime.now().timestamp(),
                'agent_email': get_gitconfig('user.name'),
                'agent_name': get_gitconfig('user.email'),
            }
        ],
    }
    return source


def new_dataset_meta_item(ds_id, ds_version, ds_name = '', ds_description = ''):
    """Create a minimal valid dataset metadata blob in catalog schema"""
    meta_item = {
        'type': 'dataset',
        'dataset_id': ds_id,
        'dataset_version': ds_version,
        'name': ds_name,
        'description': ds_description,
        'metadata_sources': get_metadata_source(),
    }
    return meta_item


def new_file_meta_item(ds_id, ds_version, filepath, content_bytesize = None, url = None):
    """Create a minimal valid dataset metadata blob in catalog schema"""
    meta_item = {
        'type': 'file',
        'dataset_id': ds_id,
        'dataset_version': ds_version,
        'path': filepath,
        'metadata_sources': get_metadata_source(),
    }
    return meta_item

# -----

if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        '-m', '--metadata', metavar='METADATA', required=True,
        help="Path to the metadata file. "
        "Metadata can be provided for datasets or files "
        "and should contain content that adheres to the schema "
        "provided at ..."
    )
    p.add_argument(
        '-t', '--type', metavar='TYPE', required=True,
        choices=['dataset', 'file'],
        help="The type of metadata file supplied."
        "The value should be either 'dataset' or 'file.'"
    )
    args = p.parse_args()
    main(
        metadata=args.metadata,
        metadata_type=args.type,
    )
