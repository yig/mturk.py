#!/usr/bin/env python3 -u

'''
Author: Yotam Gingold <yotam@yotamgingold.com>
Home: https://github.com/yig/mturk.py

Any copyright is dedicated to the Public Domain.
http://creativecommons.org/publicdomain/zero/1.0/
'''

import boto3
from datetime import datetime
import xml.dom.minidom

## From: https://stackoverflow.com/questions/54198700/how-to-delete-still-available-hits-using-boto3-client
def expire_hit( mturk, HITId ):
    mturk.update_expiration_for_hit(
        HITId = HITId,
        ExpireAt = datetime(2015, 1, 1)
        )

## From: https://stackoverflow.com/questions/46692234/how-to-submit-mechanical-turk-externalquestions-with-boto3/56903989#56903989
class ExternalQuestion:
    """
    An object for constructing an External Question.
    """
    schema_url = "http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2006-07-14/ExternalQuestion.xsd"
    template = '<ExternalQuestion xmlns="%(schema_url)s"><ExternalURL>%%(external_url)s</ExternalURL><FrameHeight>%%(frame_height)s</FrameHeight></ExternalQuestion>' % vars()

    def __init__(self, external_url, frame_height):
        self.external_url = external_url
        self.frame_height = frame_height

    def get_as_params(self, label='ExternalQuestion'):
        return {label: self.get_as_xml()}

    def get_as_xml(self):
        return self.template % vars(self)

def create_mturk( sandbox = True ):
    
    ## From: https://stackoverflow.com/questions/43013914/how-to-connect-to-mturk-sandbox-with-boto3
    endpoint_url = ( 'https://mturk-requester-sandbox.us-east-1.amazonaws.com' if sandbox else 'https://mturk-requester.us-east-1.amazonaws.com' )
    print('[MTurkConnection( %s )]' % (endpoint_url,))
    
    ## Set the credentials with a config file in "~/.aws/credentials":
    '''
    [Credentials]
    aws_access_key_id = <your access key>
    aws_secret_access_key = <your secret key>
    '''
    ## or with environment variables.
    ## https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration
    
    mturk = boto3.client(
        'mturk',
        
        #aws_access_key_id = '...',
        #aws_secret_access_key = '...',
        
        endpoint_url = endpoint_url,
        
        ## This shouldn't be necessary, because we specify a full endpoint_url, but it is.
        region_name = 'us-east-1'
        )
    
    ## With debug set to 2, all requests are printed to stdout.
    ## I can examine it by copying and pasting it into a python
    ## console and calling urllib.unquote() on it.
    # debug = 1
    ## UPDATE: boto3 doesn't use a debug parameter when opening the connection.
    ## From: https://stackoverflow.com/questions/29929540/how-to-view-boto3-https-request-string
    boto3.set_stream_logger( name = 'botocore' )
    import logging
    logging.getLogger('botocore').setLevel( logging.INFO )
    
    return mturk

def total_payment_from_worker_payment( amount, max_assignments ):
    '''
    Given a floating-point amount to pay a worker for completing a HIT in US dollars,
    returns the total amount that will be deducted from the Amazon account
    once Amazon's overhead is taken as well.
    '''
    
    ## The formula for Amazon's overhead is 10% or .005 per HIT, whichever is larger:
    return amount + max( .01, .2*amount if max_assignments < 10 else .4*amount )


def create_HITs_for_external_URLs( mturk, URLs, **kwargs ):
    '''
    Given 'mturk', an object returned from create_mturk(),
    a sequence of URL strings 'URLs',
    a keyword argument 'frame_height' specifying the height of the frame
    in which the URL will be displayed to workers,
    a dollar amount 'Reward' (minimum ".01") to pay workers,
    and the various other keyword arguments to
    boto3.client('mturk').create_hit(),
    creates one HIT per URL and returns the resulting HIT objects
    (which have a .HITId field).
    
    NOTE: The keyword argument 'RequesterAnnotation', if present will be used
          for every HIT.
          If the keyword argument 'RequesterAnnotations' is present instead,
          it must be a sequence the same length as 'URLs', and the
          corresponding element in 'RequesterAnnotations' will be used as the
          annotation for each URL in 'URLs'.
          Neither, one, or the other or 'RequesterAnnotation' and 'RequesterAnnotations'
          may be present, but not both.
    '''
    
    kwargs = dict( kwargs )
    
    frame_height = kwargs['frame_height']
    del kwargs['frame_height']
    
    ## 'annotation' and 'annotations' cannot both be in kwargs.
    assert not ( 'RequesterAnnotation' in kwargs and 'RequesterAnnotations' in kwargs )
    if 'RequesterAnnotation' in kwargs:
        RequesterAnnotations = [ kwargs['RequesterAnnotation'] ] * len( URLs )
        del kwargs['RequesterAnnotation']
    elif 'RequesterAnnotations' in kwargs:
        RequesterAnnotations = kwargs['RequesterAnnotations']
        del kwargs['RequesterAnnotations']
    else:
        RequesterAnnotations = [ '' ] * len( URLs )
    
    if len( URLs ) == 0:
        print('create_HITs_for_external_URLs() called with zero URLs')
        return []
    
    if not have_enough_balance_for_N_assignments_at_P_dollars_amount( mturk, len( URLs ) * kwargs['MaxAssignments'], kwargs['Reward'] ):
        raise RuntimeError('Not enough balance!')
    
    
    HITs = []
    assert len( URLs ) == len( RequesterAnnotations )
    for URL, RequesterAnnotation in zip( URLs, RequesterAnnotations ):
        Question = ExternalQuestion( URL, frame_height ).get_as_xml()
        
        create_hit_result = mturk.create_hit(
            Question = Question,
            ## The default for create_hit() is Minimal;
            ## also add 'HITDetail' for CreationTime,
            ## 'HITQuestion' for ExternalURL and FrameHeight,
            ## and 'HITAssignmentSummary' for NumberofAssignmentsPending,
            ## NumberofAssignmentsAvailable, or NumberofAssignmentsCompleted.
            # response_groups = ( 'Minimal', 'HITDetail', 'HITQuestion', 'HITAssignmentSummary' ),
            ## The default is Minimal; also get HITDetail for CreationTime.
            # response_groups = ( 'Minimal', 'HITDetail' ),
            RequesterAnnotation = RequesterAnnotation,
            **kwargs
            )
        
        hit = create_hit_result['HIT']
        HITs.append( hit )
        
        print('[create_hit( %s, $%s ): %s]' % ( URL, kwargs['Reward'], hit['HITId'] ))
    
    return HITs

def create_HIT_for_external_URL( mturk, URL, **kwargs ):
    '''
    Given 'mturk', an object returned from create_mturk(),
    a URL strings 'URL',
    a keyword argument 'frame_height' specifying the height of the frame
    in which the URL will be displayed to workers,
    a dollar amount 'amount' (minimum .01) to pay workers,
    and the various other keyword arguments to
    boto3.mturk.connection.MTurkConnection.create_hit(),
    creates one HIT per URL and returns the resulting HIT objects
    (which have a .HITId field).
    '''
    
    frame_height = kwargs['frame_height']
    del kwargs['frame_height']
    
    if not have_enough_balance_for_N_assignments_at_P_dollars_amount( mturk, kwargs['MaxAssignments'], kwargs['Reward'] ):
        raise RuntimeError('Not enough balance!')
    
    
    Question = ExternalQuestion( URL, frame_height ).get_as_xml()
    
    create_hit_result = mturk.create_hit(
        Question = Question,
        # response_groups = ( 'Minimal', 'HITDetail' ), ## The default is Minimal; also get HITDetail for CreationTime.
        **kwargs
        )
    
    HIT = create_hit_result['HIT']
    print('[create_hit( %s, $%s ): %s]' % ( URL, kwargs['Reward'], hit['HITId'] ))
    
    return HIT

def have_enough_balance_for_N_assignments_at_P_dollars_amount( mturk, N, P ):
    P = float(P)
    return float(mturk.get_account_balance()['AvailableBalance']) >= total_payment_from_worker_payment( P, N )

def get_assignments_for_HITId( mturk, HITId, max_assignments ):
    assignments = mturk.list_assignments_for_hit( HITId = HITId, MaxResults = max_assignments, AssignmentStatuses = [ 'Submitted', 'Approved', 'Rejected' ] )
    assert assignments['NumResults'] == max_assignments
    ## Make sure that NumResults and TotalNumResults agree; if they don't,
    ## we need to properly handle the 'page_number', and we're not right now.
    ## assignments.NumResults and assignments.TotalNumResults are given as strings.
    assert int( assignments['NumResults'] ) == len( assignments['Assignments'] )
    print('[get_assignments_for_HITId( %s, %s ): %d assignments]' % ( HITId, max_assignments, len( assignments['Assignments'] ) ))
    return assignments['Assignments']

def get_all_assignments_for_HITId( mturk, HITId ):
    '''
    Returns all Assignment objects for the given HITId.
    
    tested
    '''
    
    ## NOTE: This routine has been tested for N equal to 1, 10, and 100
    ##       for a HITId that had 10 assignments.
    N = 100
    NextToken = None
    results = []
    while True:
        kwargs = dict( HITId = HITId, MaxResults = N, NextToken = NextToken )
        if NextToken is None: del kwargs['NextToken']
        assignments = mturk.list_assignments_for_hit( **kwargs )
        results.extend( assignments['Assignments'] )
        ## Make sure that NumResults and TotalNumResults agree; if they don't,
        ## we need to properly handle the 'page_number', and we're not right now.
        ## assignments.NumResults and assignments.TotalNumResults are given as strings.
        assert int( assignments['NumResults'] ) == len( assignments['Assignments'] )
        #assert int( assignments.NumResults ) == int( assignments.TotalNumResults )
        if len( assignments['Assignments'] ) == 0: break
        NextToken = assignments['NextToken']
    print('[get_all_assignments_for_HITId( %s ): %d assignments]' % ( HITId, len( results ) ))
    return results

def remove_HITId( mturk, HITId ):
    '''
    Removes the given HITId, approving any pending reviewable assignments.
    '''
    
    HITobj = mturk.get_hit( HITId = HITId )['HIT']
    print('Removing HITId %s with current status %s' % ( HITId, HITobj['HITStatus'] ))
    
    if HITobj['HITStatus'] == 'Disposed':
        return
    elif HITobj['HITStatus'] == 'Reviewable':
        assignments = get_all_assignments_for_HITId( mturk, HITId )
        print('Approving reviewable assignments...')
        num_approved = 0
        for assignment in assignments:
            if assignment.AssignmentStatus == 'Submitted':
                mturk.approve_assignment( AssignmentId = assignment.AssignmentId )
                num_approved += 1
        print('Approved', num_approved, 'assignments.' if num_approved != 1 else 'assignment.')
        
        mturk.delete_hit( HITId = HITId )
    else:
        ## UPDATE: There is no more disable_hit()
        # mturk.disable_hit( HITId )
        expire_hit( mturk, HITId )

def HITIds2HITs( mturk, HITIds ):
    '''
    Given a sequence of HITIds, return corresponding
    boto3.mturk.connection.HIT objects.
    '''
    
    HITs = []
    for HITId in HITIds:
        get_hit_result = mturk.get_hit(
            HITId = HITId
            ## The default for get_hit() is HITDetail, HITQuestion and Minimal;
            ## also add HITAssignmentSummary for NumberofAssignmentsPending,
            ## NumberofAssignmentsAvailable, or NumberofAssignmentsCompleted.
            # response_groups = ( 'Minimal', 'HITDetail', 'HITQuestion', 'HITAssignmentSummary' ),
            )
        
        hit = get_hit_result['HIT']
        # assert get_hit_result.status
        HITs.append( hit )
    
    return HITs

def HITIds2CSV( mturk, HITIds ):
    return HITs2CSV( HITIds2HITs( mturk, HITIds ) )

def HITs2CSV( HITs ):
    '''
    Given a sequence of boto3.mturk.connection.HIT objects, as returned by
    boto3.mturk.connection.MTurkConnection.create_hit() or
    boto3.mturk.connection.MTurkConnection.get_hit(),
    returns a string of CSV data representing the assignments.
    
    NOTE: In order to get the CreationTime field,
          it is necessary to have passed 'HITDetail'
          as an element of 'response_groups':
              response_groups = ( 'Minimal', 'HITDetail' )
          to create_hit() or get_hit().
    
    NOTE: In order to get the NumberofAssignmentsPending,
          NumberofAssignmentsAvailable, or NumberofAssignmentsCompleted
          fields, it is necessary to have passed 'HITAssignmentSummary'
          as an element of 'response_groups':
              response_groups = ( 'Minimal', 'HITDetail', 'HITAssignmentSummary' )
          to create_hit() or get_hit().
    
    NOTE: In order to get the ExternalURL and FrameHeight fields,
          it is necessary to have passed 'HITQuestion'
          as an element of 'response_groups':
              response_groups = ( 'Minimal', 'HITDetail', 'HITQuestion', 'HITAssignmentSummary' )
          to create_hit() or get_hit().
    '''
    
    import sys, csv, io
    
    ## Column names:
    primary_fields = [ 'HITId', 'HITTypeId', 'CreationTime', 'Title', 'Description', 'Keywords', 'HITStatus', 'Reward', 'LifetimeInSeconds', 'AssignmentDurationInSeconds', 'MaxAssignments', 'AutoApprovalDelayInSeconds', 'ExternalURL', 'FrameHeight', 'RequesterAnnotation', 'NumberOfSimilarHITs', 'HITReviewStatus', 'NumberofAssignmentsPending', 'NumberofAssignmentsAvailable', 'NumberofAssignmentsCompleted' ]
    
    ## A sequence of dictionaries mapping field names to values.
    rows = []
    for hit in HITs:
        row = {}
        
        for field in primary_fields:
            ## There are a few special cases:
            if field == 'ExternalURL':
                if 'Question' in hit:
                    
                    '''
                    d = xml.dom.minidom.parseString( hit.Question )
                    ExternalURL = d.getElementsByTagName( 'ExternalURL' )[0].firstChild.data
                    row[ field ] = ExternalURL
                    '''
                    row[ field ] = xml.dom.minidom.parseString( hit['Question'] ).getElementsByTagName( 'ExternalURL' )[0].firstChild.data
            elif field == 'FrameHeight':
                if 'Question' in hit:
                    
                    '''
                    d = xml.dom.minidom.parseString( hit.Question )
                    FrameHeight = d.getElementsByTagName( 'FrameHeight' )[0].firstChild.data
                    row[ field ] = FrameHeight
                    '''
                    row[ field ] = xml.dom.minidom.parseString( hit['Question'] ).getElementsByTagName( 'FrameHeight' )[0].firstChild.data
            ## The general case:
            elif field in hit:
                row[ field ] = hit[ field ]
        
        rows.append( row )
    
    out = io.StringIO()
    dw = csv.DictWriter( out, primary_fields, lineterminator = '\n' )
    dw.writeheader()
    dw.writerows( rows )
    return out.getvalue()

def assignments2CSV( assignments ):
    '''
    Given a sequence of boto3.mturk.connection.Assignment objects as returned by
    boto3.mturk.connection.MTurkConnection.get_assignments(),
    returns a string of CSV data representing the assignments.
    '''
    
    import sys, csv, io, json
    
    ## Column names:
    primary_fields = [ 'HITId', 'AssignmentId', 'WorkerId', 'AssignmentStatus', 'AutoApprovalTime', 'AcceptTime', 'SubmitTime', 'ApprovalTime', 'RejectionTime', 'Deadline', 'RequesterFeedback' ]
    qid_fields = set()
    
    ## A sequence of dictionaries mapping field names to values.
    rows = []
    for a in assignments:
        row = {}
        
        for field in primary_fields:
            if field in a:
                row[ field ] = a[field]
        
        ### Now, question ids.
        ## My older forked boto:
        #answers = [ ( answer.QuestionIdentifier, answer.FreeText ) for answer in a.answers ]
        ## My current forked boto:
        # answers = [ ( answer.qid, answer.fields ) for answer in a.answers[0] ]
        ## UPDATE: boto3 doesn't parse this for us.
        answersdom = xml.dom.minidom.parseString( a['Answer'] )
        # def getData( node ): return node.data if node.nodeType == node.TEXT_NODE else node.firstChild.data
        ## ET.fromstring(answersdom.getElementsByTagName( 'Answer' )[0].toxml()))[0].text
        answers = [ ( a.childNodes[0].firstChild.data, a.childNodes[1].firstChild.data ) for a in answersdom.getElementsByTagName( 'Answer' ) ]
        
        for qid, fields in answers:
            qid_fields.add( qid )
            
            assert qid not in row
            row[ qid ] = json.dumps( fields )
        
        rows.append( row )
    
    ## Combine the primary column names with the question ids:
    qid_fields = list( qid_fields )
    qid_fields.sort()
    all_fields = primary_fields + qid_fields
    
    out = io.StringIO()
    dw = csv.DictWriter( out, all_fields, lineterminator = '\n' )
    dw.writeheader()
    dw.writerows( rows )
    return out.getvalue()

def upload_filepaths_to_server(
    filepaths, remote_host = None, remote_dir = None
    ):
    '''
    Given an iterable collection of distinct file paths 'filepaths' and
    a remote host 'remote_host' and remote directory 'remote_dir'
    in which to store the files on the server,
    uploads the files intelligently (doesn't re-upload if the
    identical file is already on the 'remote_host' at 'remote_dir').
    '''
    
    import os, subprocess
    
    filepaths = list( set( filepaths ) )
    if len( filepaths ) == 0:
        print('upload_filepaths_to_server() called with zero filepaths.')
        return
    
    ## A filepath shouldn't appear twice in the input list.
    assert len( set( filepaths ) ) == len( filepaths )
    ## All paths in 'filepaths' should be to files.
    assert all([ os.path.isfile( filepath ) for filepath in filepaths ])
    
    assert remote_host is not None
    assert remote_dir is not None
    
    assert str( remote_host ) == remote_host
    assert str( remote_dir ) == remote_dir
    
    ## Create 'remote_dir' if it doesn't exist.
    ## UPDATE: Calling this often can introduce unnecessary pauses,
    ##         because chances are the directory exists.
    print('[ssh "%s" mkdir -p "%s"]' % ( remote_host, remote_dir ))
    err = subprocess.Popen( [ 'ssh', remote_host, 'mkdir', '-p', remote_dir ] ).wait()
    assert 0 == err
    
    ## Upload all the files together in the foreground.
    ## NOTE: I think the trailing "/" will cause scp
    ##       to abort with an error code if "remote_dir" is not a directory.
    #print '[scp "%s" "%s"]' % ( filepaths, remote_host + ':' + remote_dir + '/' )
    #err = os.spawnvp( os.P_WAIT, 'scp', [ 'scp' ] + filepaths + [ remote_host + ':' + remote_dir + '/' ] )
    ## UPDATE: Better yet, use rsync.
    print('[rsync "%s" "%s"]' % ( filepaths, remote_host + ':' + remote_dir + '/' ))
    err = os.spawnvp( os.P_WAIT, 'rsync', [ 'rsync', '--progress' ] + filepaths + [ remote_host + ':' + remote_dir + '/' ] )
    assert 0 == err


def main():
    import sys, json
    
    def usage():
        print('Usage:', sys.argv[0], '[really] submit path/to/job.json', file=sys.stderr)
        print('Usage:', sys.argv[0], '[really] info HITId', file=sys.stderr)
        print('Usage:', sys.argv[0], '[really] retrieve HITId', file=sys.stderr)
        print('Usage:', sys.argv[0], '[really] approve AssignmentId [feedback]', file=sys.stderr)
        print('Usage:', sys.argv[0], '[really] reject AssignmentId [feedback]', file=sys.stderr)
        print('Usage:', sys.argv[0], '[really] bonus WorkerId AssignmentId dollars feedback', file=sys.stderr)
        print('Usage:', sys.argv[0], '[really] extend HITId number-of-additional-assignments', file=sys.stderr)
        print('Usage:', sys.argv[0], '[really] expire HITId', file=sys.stderr)
        print('Usage:', sys.argv[0], '[really] remove HITId', file=sys.stderr)
        ## TODO:
        #print >> sys.stderr, 'Usage:', sys.argv[0], 'extend HITId additional_assignments ?additional_time?'
        
        print('Example:', sys.argv[0], 'submit debug.json', file=sys.stderr)
        
        print('Example "debug.json":', file=sys.stderr)
        print('''{
    "create_hit_kwargs": {
        "title": "Nothing",
        "description": "Testing parameters in and out of sandbox.",
        "keywords": [ "debug, debugging" ],
        
        "frame_height": 900,
        
        "amount": 0.1,
        "max_assignments": 10,
        
        "qualifications": [
            [ "PercentAssignmentsApprovedRequirement", "GreaterThan", "95" ],
            [ "NumberHitsApprovedRequirement", "GreaterThan", "100" ],
            [ "LocaleRequirement", "EqualTo", "US" ]
            ],
        
        "duration": 360,
        "lifetime": 604800,
        "approval_delay": 0
        },
    "URLs": [ "http://example.com/page.html" ]
}''', file=sys.stderr)
        
        print('Note: Commands run in the sandbox unless "really" is present.', file=sys.stderr)
        print('Note: The "qualifications" field is optional.  The default is to have no qualifications.  Any qualification type supported by boto3 is allowed.', file=sys.stderr)
        
        sys.exit(-1)
    
    def submit( argv ):
        if len( argv ) != 1: usage()
        
        params_path = argv[0]
        params = json.load( open( params_path ) )
        
        create_hit_kwargs = params['create_hit_kwargs']
        del params['create_hit_kwargs']
        
        URLs = params['URLs']
        del params['URLs']
        
        if len( params ) > 0: usage()
        
        create_HITs_for_external_URLs( mturk, URLs, **create_hit_kwargs )
    
    def info( argv ):
        if len( argv ) != 1: usage()
        
        HITId = argv[0]
        
        print(HITIds2CSV( mturk, [ HITId ] ), end=' ')
    
    def retrieve( argv ):
        if len( argv ) != 1: usage()
        
        HITId = argv[0]
        
        assignments = get_all_assignments_for_HITId( mturk, HITId )
        print(assignments2CSV( assignments ), end=' ')
    
    def expire( argv ):
        if len( argv ) != 1: usage()
        
        HITId = argv[0]
        
        expire_hit( mturk, HITId )
    
    def remove( argv ):
        if len( argv ) != 1: usage()
        
        HITId = argv[0]
        
        remove_HITId( mturk, HITId )
    
    def approve( argv ):
        if len( argv ) not in (1,2): usage()
        
        AssignmentId = argv.pop(0)
        
        feedback = None if len( argv ) == 0 else argv[0]
        
        mturk.approve_assignment( AssignmentId = AssignmentId, RequesterFeedback = feedback )
    
    def reject( argv ):
        if len( argv ) not in (1,2): usage()
        
        AssignmentId = argv.pop(0)
        
        feedback = None if len( argv ) == 0 else argv[0]
        
        mturk.reject_assignment( AssignmentId = AssignmentId, RequesterFeedback = feedback )
    
    def extend( argv ):
        if len( argv ) != 2: usage()
        
        HITId, number_of_additional_assignments = argv
        
        try:
            number_of_additional_assignments = int( number_of_additional_assignments )
        except ValueError: usage()
        if number_of_additional_assignments < 0: usage()
        
        print('[extend_hit( %s, %d additional assignments )]' % ( HITId, number_of_additional_assignments ))
        mturk.create_additional_assignments_for_hit( HITId = HITId, NumberOfAdditionalAssignments = number_of_additional_assignments )
    
    def bonus( argv ):
        if len( argv ) != 4: usage()
        
        WorkerId, AssignmentId, BonusAmount, Reason = argv
        mturk.send_bonus( WorkerId = WorkerId, AssignmentId = AssignmentId, BonusAmount = BonusAmount, Reason = Reason )
    
    def debug( argv ):
        print('sandbox:', sandbox)
    
    argv = list( sys.argv )
    del argv[0]
    
    if len( argv ) == 0: usage()
    
    sandbox = True
    if 'really' == argv[0]:
        sandbox = False
        del argv[0]
    mturk = create_mturk( sandbox = sandbox )
    
    if len( argv ) == 0: usage()
    
    commands = [ submit, info, retrieve, approve, reject, bonus, extend, expire, remove, debug ]
    name2func = dict([ ( f.__name__, f ) for f in commands ])
    
    try:
        func = name2func[ argv[0] ]
    except KeyError:
        usage()
    
    func( argv[1:] )

if __name__ == '__main__': main()
