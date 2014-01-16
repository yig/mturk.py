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

def get_columns_from_csv_file_object( column_names, csv_file_object ):
    result = [
        [ ( line[ column_name ] if column_name in line else None ) for column_name in column_names ]
        for line in csv.DictReader( csv_file_object )
        ]
    
    if None in result:
        ## If the column name isn't in one row, it shouldn't be in any rows.
        assert all([ e is None for e in result ])
        raise KeyError( 'Column name does not exist: ' + column_name )
    
    return result

def get_columns_from_csv_path( column_names, csv_path ):
    return get_columns_from_csv_file_object( column_names, open( csv_path, 'rU' ) )

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
        print >> sys.stderr, 'Usage:', sys.argv[0], 'path/to/file.csv column_name [column_name2 ...]'
        sys.exit(-1)
    
    try:
        csv_path, column_names = sys.argv[1], sys.argv[2:]
    except:
        usage()
    
    ## In case the user thinks they can pass multiple CSV files instead of multiple column names.
    if len( column_names ) > 1 and all([ name.lower().endswith('.csv') for name in column_names[:-1] ]):
        usage()
    
    if csv_path == '-':
        columns = get_columns_from_csv_file_object( column_names, sys.stdin )
    
    else:
        if not os.path.exists( csv_path ):
            usage()
        
        columns = get_columns_from_csv_path( column_names, csv_path )
    
    ## Transpose the column into a row:
    # print ','.join( column )
    
    # for el in columns: print el
    csv.writer( sys.stdout, lineterminator = '\n' ).writerows( columns )

if __name__ == '__main__': main()