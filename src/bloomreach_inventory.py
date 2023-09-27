import gzip
import json
import jsonlines
import logging

logger = logging.getLogger(__name__)

def create_patch_from_products_fp(fp_in):
  patch = []

  with gzip.open(fp_in, "rb") as file:
    for line in file:
      patch.append(create_add_product_op(json.loads(line)))
  
  return patch

# construct an add product operation from Bloomreach Discovery product
def create_add_product_op(input_data):

  br_product_id = input_data['value']['attributes']['spm.custom.br_product_id']

  if br_product_id is not None:
    return {
      "op": "add",
      "path": f"/products/br_product_id/attributes/totalInventory",
      "value": input_data['value']['attributes']['sp.totalInventory']
    }
  else:
    return

def main(fp_in, fp_out):
  patch = create_patch_from_products_fp(fp_in)

  from sys import stdout
  
  # Define logger
  loglevel = getenv('LOGLEVEL', 'INFO').upper()
  logging.basicConfig(
    stream=stdout, 
    level=loglevel,
    format="%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s"
  )
  
  # write JSONLines to stdout
  with gzip.open(fp_out, "wb") as file:
    writer = jsonlines.Writer(file)
    for object in patch:
      writer.write(object)
    writer.close()

if __name__ == '__main__':
  import argparse
  from os import getenv
  
  parser = argparse.ArgumentParser(
    description="Extracts totalInventory from Bloomreach Discovery catalog file. This patch can be used as a Full or Delta feed data source either directly in API request or SFTP."
  )
  
  parser.add_argument(
    "--input-file",
    help="File path of Bloomreach Products jsonl",
    type=str,
    default=getenv("BR_INPUT_FILE"),
    required=not getenv("BR_INPUT_FILE")
  )

  parser.add_argument(
    "--output-file",
    help="Filename of output jsonl file",
    type=str,
    default=getenv("BR_OUTPUT_FILE"),
    required=not getenv("BR_OUTPUT_FILE")
  )

  args = parser.parse_args()
  fp_in = args.input_file
  fp_out = args.output_file

  main(fp_in, fp_out)
  
