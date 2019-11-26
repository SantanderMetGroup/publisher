#!/usr/bin/env python

import os, sys
import argparse

from jinja2 import Environment, FileSystemLoader, select_autoescape

def generate(template, **kwargs):
	templates = os.path.join(os.path.dirname(__file__), 'data')
	env = Environment(loader=FileSystemLoader(templates), autoescape=select_autoescape(['xml']))
	template = env.get_template(template)

	return template.render(**kwargs)

def main(args):
	files = sys.stdin.read().splitlines()
	ncmls = [f.replace(args.root, '').lstrip('/') for f in files]
	params = {
		'name': args.name,
		'location': args.location,
		'path': args.path,
		'ncmls': ncmls
	}

	print(generate(args.template, **params))

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Create a catalog from a list of ncmls.')
	parser.add_argument('--root', dest='root', type=str, help='Root of NcMLs hierarchy.')
	parser.add_argument('--name', dest='name', type=str, help='Catalog name.')
	parser.add_argument('--location', dest='location', type=str, help='DatasetRoot location.')
	parser.add_argument('--path', dest='path', type=str, help='DatasetRoot path.')
	parser.add_argument('--template', dest='template', type=str, help='Template to use.')

	args = parser.parse_args()

	main(args)
