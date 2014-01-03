#!/usr/bin/env python

'''
A convenience script to extract a single named column from a CSV file.

Usage: ./csv_helpers.py path/to/file.csv column_name


Author: Yotam Gingold <yotam@yotamgingold.com>

Any copyright is dedicated to the Public Domain.
http://creativecommons.org/publicdomain/zero/1.0/
'''

import csv
## Up the field size limit, because sometimes I store base64-encoded PNG's in the columns.
csv.field_size_limit( 1310720 )

def get_column_from_csv_file_object( column_name, csv_file_object ):
    result = [
        line[ column_name ] if column_name in line else None
        for line in csv.DictReader( csv_file_object )
        ]
    
    if None in result:
        ## If the column name isn't in one row, it shouldn't be in any rows.
        assert all([ e is None for e in result ])
        raise KeyError( 'Column name does not exist: ' + column_name )
    
    return result

def get_column_from_csv_path( column_name, csv_path ):
    return get_column_from_csv_file_object( column_name, open( csv_path, 'rU' ) )

def get_lines_matching_column_values_from_csv_path( column_name2values, csv_path ):
    return [
        line for line in csv.DictReader( open( csv_path ) )
        if all([
            line[name] == value
            for name, value in column_name2values.iteritems()
            ])
        ]

def main():
    import os, sys
    
    def usage():
        print >> sys.stderr, 'Usage:', sys.argv[0], 'path/to/file.csv column_name'
        sys.exit(-1)
    
    try:
        csv_path, column_name = sys.argv[1:]
    except:
        usage()
    
    if csv_path == '-':
        column = get_column_from_csv_file_object( column_name, sys.stdin )
    
    else:
        if not os.path.exists( csv_path ):
            usage()
        
        column = get_column_from_csv_path( column_name, csv_path )
    
    ## Transpose the column into a row:
    # print ','.join( column )
    
    for el in column: print el

if __name__ == '__main__': main()
