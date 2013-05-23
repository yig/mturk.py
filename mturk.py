#!/usr/bin/env python2.7 -u

'''
Author: Yotam Gingold <yotam@yotamgingold.com>

Any copyright is dedicated to the Public Domain.
http://creativecommons.org/publicdomain/zero/1.0/
'''

import boto.mturk.connection, boto.mturk.question, boto.mturk.price, boto.mturk.qualification


def create_mturk( sandbox = True ):
    
    host = ( 'mechanicalturk.sandbox.amazonaws.com' if sandbox else 'mechanicalturk.amazonaws.com' )
    print '[MTurkConnection( %s )]' % (host,)
    
    ## Set the credentials with a config file in "~/.boto":
    '''
    [Credentials]
    aws_access_key_id = <your access key>
    aws_secret_access_key = <your secret key>
    '''
    ## or with environment variables.
    ## http://readthedocs.org/docs/boto/en/latest/boto_config_tut.html
    
    mturk = boto.mturk.connection.MTurkConnection(
        #aws_access_key_id = '...',
        #aws_secret_access_key = '...',
        
        host = host,
        
        ## Q: Do I need to set the secure socket factory parameter if I set 'is_secure'?
        ## A: It looks like I don't, because just setting is_secure = True works.
        is_secure = True,
        
        ## With debug set to 2, all requests are printed to stdout.
        ## I can examine it by copying and pasting it into a python
        ## console and calling urllib.unquote() on it.
        debug = 1
        )
    
    return mturk

def total_payment_from_worker_payment( amount ):
    '''
    Given an amount to pay a worker for completing a HIT in US dollars,
    returns the total amount that will be deducted from the Amazon account
    once Amazon's overhead is taken as well.
    '''
    
    ## The formula for Amazon's overhead is 10% or .005 per HIT, whichever is larger:
    return amount + max( .005, .1*amount )


def create_HITs_for_external_URLs( mturk, URLs, **kwargs ):
    '''
    Given 'mturk', an object returned from create_mturk(),
    a sequence of URL strings 'URLs',
    a keyword argument 'frame_height' specifying the height of the frame
    in which the URL will be displayed to workers,
    a dollar amount 'amount' (minimum .01) to pay workers,
    and the various other keyword arguments to
    boto.mturk.connection.MTurkConnection.create_hit(),
    creates one HIT per URL and returns the resulting HIT objects
    (which have a .HITId field).
    
    NOTE: The keyword argument 'annotation', if present will be used
          for every HIT.
          If the keyword argument 'annotations' is present instead,
          it must be a sequence the same length as 'URLs', and the
          corresponding element in 'annotations' will be used as the
          annotation for each URL in 'URLs'.
          Neither, one, or the other or 'annotation' and 'annotations'
          may be present, but not both.
    '''
    
    kwargs = dict( kwargs )
    
    frame_height = kwargs['frame_height']
    del kwargs['frame_height']
    
    amount = kwargs['amount']
    del kwargs['amount']
    
    qualifications = None
    if 'qualifications' in kwargs:
        qualifications_pretty = kwargs['qualifications']
        del kwargs['qualifications']
        if qualifications_pretty is not None:
            
            ## Get all supported qualification classes.
            import inspect
            name2class = dict([
                ( name, cls )
                for name, cls in inspect.getmembers( boto.mturk.qualification, inspect.isclass )
                if issubclass( cls, boto.mturk.qualification.Requirement ) and cls is not boto.mturk.qualification.Requirement
                ])
            
            
            ## Iterate over the qualifications, and add them to a set.
            qualifications = boto.mturk.qualification.Qualifications()
            
            for qual_pretty in qualifications_pretty:
                print 'Qualification:', qual_pretty
                qualifications.add( name2class[ qual_pretty[0] ]( *qual_pretty[1:] ) )
    
    ## 'annotation' and 'annotations' cannot both be in kwargs.
    assert not ( 'annotation' in kwargs and 'annotations' in kwargs )
    if 'annotation' in kwargs:
        annotations = [ kwargs['annotation'] ] * len( URLs )
        del kwargs['annotation']
    elif 'annotations' in kwargs:
        annotations = kwargs['annotations']
        del kwargs['annotations']
    else:
        annotations = [ None ] * len( URLs )
    
    if len( URLs ) == 0:
        print 'create_HITs_for_external_URLs() called with zero URLs'
        return []
    
    if not have_enough_balance_for_N_assignments_at_P_dollars_amount( mturk, len( URLs ) * kwargs['max_assignments'], amount ):
        raise RuntimeError, 'Not enough balance!'
    
    
    HITs = []
    assert len( URLs ) == len( annotations )
    for URL, annotation in zip( URLs, annotations ):
        questionform = boto.mturk.question.ExternalQuestion( URL, frame_height )
        
        create_hit_result = mturk.create_hit(
            question = questionform,
            reward = boto.mturk.price.Price( amount = amount, currency_code = 'USD' ),
            qualifications = qualifications,
            ## The default for create_hit() is Minimal;
            ## also add 'HITDetail' for CreationTime,
            ## 'HITQuestion' for ExternalURL and FrameHeight,
            ## and 'HITAssignmentSummary' for NumberofAssignmentsPending,
            ## NumberofAssignmentsAvailable, or NumberofAssignmentsCompleted.
            response_groups = ( 'Minimal', 'HITDetail', 'HITQuestion', 'HITAssignmentSummary' ),
            ## The default is Minimal; also get HITDetail for CreationTime.
            # response_groups = ( 'Minimal', 'HITDetail' ),
            annotation = annotation,
            **kwargs
            )
        
        hit = create_hit_result[0]
        assert create_hit_result.status
        HITs.append( hit )
        
        print '[create_hit( %s, $%s ): %s]' % ( URL, amount, hit.HITId )
    
    return HITs

def create_HIT_for_external_URL( mturk, URL, **kwargs ):
    '''
    Given 'mturk', an object returned from create_mturk(),
    a URL strings 'URL',
    a keyword argument 'frame_height' specifying the height of the frame
    in which the URL will be displayed to workers,
    a dollar amount 'amount' (minimum .01) to pay workers,
    and the various other keyword arguments to
    boto.mturk.connection.MTurkConnection.create_hit(),
    creates one HIT per URL and returns the resulting HIT objects
    (which have a .HITId field).
    '''
    
    frame_height = kwargs['frame_height']
    del kwargs['frame_height']
    
    amount = kwargs['amount']
    del kwargs['amount']
    
    if not have_enough_balance_for_N_assignments_at_P_dollars_amount( mturk, kwargs['max_assignments'], amount ):
        raise RuntimeError, 'Not enough balance!'
    
    
    questionform = boto.mturk.question.ExternalQuestion( URL, frame_height )
    
    create_hit_result = mturk.create_hit(
        question = questionform,
        reward = boto.mturk.price.Price( amount = amount, currency_code = 'USD' ),
        response_groups = ( 'Minimal', 'HITDetail' ), ## The default is Minimal; also get HITDetail for CreationTime.
        **kwargs
        )
    
    HIT = create_hit_result[0]
    assert create_hit_result.status
    
    print '[create_hit( %s, $%s ): %s]' % ( URL, amount, HIT.HITId )
    
    return HIT

def have_enough_balance_for_N_assignments_at_P_dollars_amount( mturk, N, P ):
    return mturk.get_account_balance()[0].amount >= total_payment_from_worker_payment( P ) * N

def get_assignments_for_HITId( mturk, HITId, max_assignments ):
    assignments = mturk.get_assignments( HITId, page_size = max_assignments, page_number = 1 )
    assert assignments.status
    ## Make sure that NumResults and TotalNumResults agree; if they don't,
    ## we need to properly handle the 'page_number', and we're not right now.
    ## assignments.NumResults and assignments.TotalNumResults are given as strings.
    assert int( assignments.NumResults ) == len( assignments )
    assert int( assignments.NumResults ) == int( assignments.TotalNumResults )
    print '[get_assignments_for_HITId( %s, %s ): %d assignments]' % ( HITId, max_assignments, len( assignments ) )
    return assignments

def get_all_assignments_for_HITId( mturk, HITId ):
    '''
    Returns all Assignment objects for the given HITId.
    
    tested
    '''
    
    ## NOTE: This routine has been tested for N equal to 1, 10, and 100
    ##       for a HITId that had 10 assignments.
    N = 100
    i = 1
    results = []
    while True:
        assignments = mturk.get_assignments( HITId, page_size = N, page_number = i )
        assert assignments.status
        results.extend( assignments )
        ## Make sure that NumResults and TotalNumResults agree; if they don't,
        ## we need to properly handle the 'page_number', and we're not right now.
        ## assignments.NumResults and assignments.TotalNumResults are given as strings.
        assert int( assignments.NumResults ) == len( assignments )
        #assert int( assignments.NumResults ) == int( assignments.TotalNumResults )
        if len( results ) == int( assignments.TotalNumResults ): break
        i += 1
    print '[get_all_assignments_for_HITId( %s ): %d assignments]' % ( HITId, len( results ) )
    return results

def remove_HITId( mturk, HITId ):
    '''
    Removes the given HITId, approving any pending reviewable assignments.
    '''
    
    HITobj = mturk.get_hit( HITId )[0]
    print 'Removing HITId %s with current status %s' % ( HITId, HITobj.HITStatus )
    
    if HITobj.HITStatus == 'Disposed':
        return
    elif HITobj.HITStatus == 'Reviewable':
        assignments = get_all_assignments_for_HITId( mturk, HITId )
        print 'Approving reviewable assignments...'
        num_approved = 0
        for assignment in assignments:
            if assignment.AssignmentStatus == 'Submitted':
                mturk.approve_assignment( assignment.AssignmentId )
                num_approved += 1
        print 'Approved', num_approved, 'assignments.' if num_approved != 1 else 'assignment.'
        
        mturk.dispose_hit( HITId )
    else:
        mturk.disable_hit( HITId )

def HITIds2HITs( mturk, HITIds ):
    '''
    Given a sequence of HITIds, return corresponding
    boto.mturk.connection.HIT objects.
    '''
    
    HITs = []
    for HITId in HITIds:
        get_hit_result = mturk.get_hit(
            HITId,
            ## The default for get_hit() is HITDetail, HITQuestion and Minimal;
            ## also add HITAssignmentSummary for NumberofAssignmentsPending,
            ## NumberofAssignmentsAvailable, or NumberofAssignmentsCompleted.
            response_groups = ( 'Minimal', 'HITDetail', 'HITQuestion', 'HITAssignmentSummary' ),
            )
        
        hit = get_hit_result[0]
        assert get_hit_result.status
        HITs.append( hit )
    
    return HITs

def HITIds2CSV( mturk, HITIds ):
    return HITs2CSV( HITIds2HITs( mturk, HITIds ) )

def HITs2CSV( HITs ):
    '''
    Given a sequence of boto.mturk.connection.HIT objects, as returned by
    boto.mturk.connection.MTurkConnection.create_hit() or
    boto.mturk.connection.MTurkConnection.get_hit(),
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
    
    import sys, csv, StringIO
    
    ## Column names:
    primary_fields = [ 'HITId', 'HITTypeId', 'CreationTime', 'Title', 'Description', 'Keywords', 'HITStatus', 'Reward', 'LifetimeInSeconds', 'AssignmentDurationInSeconds', 'MaxAssignments', 'AutoApprovalDelayInSeconds', 'ExternalURL', 'FrameHeight', 'RequesterAnnotation', 'NumberOfSimilarHITs', 'HITReviewStatus', 'NumberofAssignmentsPending', 'NumberofAssignmentsAvailable', 'NumberofAssignmentsCompleted' ]
    
    ## A sequence of dictionaries mapping field names to values.
    rows = []
    for hit in HITs:
        row = {}
        
        for field in primary_fields:
            ## There are a few special cases:
            if field == 'Reward':
                row[ field ] = hit.FormattedPrice
            elif field == 'ExternalURL':
                if hasattr( hit, 'Question' ):
                    
                    import xml.dom.minidom
                    '''
                    d = xml.dom.minidom.parseString( hit.Question )
                    ExternalURL = d.getElementsByTagName( 'ExternalURL' )[0].firstChild.data
                    row[ field ] = ExternalURL
                    '''
                    row[ field ] = xml.dom.minidom.parseString( hit.Question ).getElementsByTagName( 'ExternalURL' )[0].firstChild.data
            elif field == 'FrameHeight':
                if hasattr( hit, 'Question' ):
                    
                    import xml.dom.minidom
                    '''
                    d = xml.dom.minidom.parseString( hit.Question )
                    FrameHeight = d.getElementsByTagName( 'FrameHeight' )[0].firstChild.data
                    row[ field ] = FrameHeight
                    '''
                    row[ field ] = xml.dom.minidom.parseString( hit.Question ).getElementsByTagName( 'FrameHeight' )[0].firstChild.data
            ## The general case:
            elif hasattr( hit, field ):
                row[ field ] = getattr( hit, field )
        
        rows.append( row )
    
    out = StringIO.StringIO()
    dw = csv.DictWriter( out, primary_fields )
    dw.writeheader()
    dw.writerows( rows )
    return out.getvalue()

def assignments2CSV( assignments ):
    '''
    Given a sequence of boto.mturk.connection.Assignment objects as returned by
    boto.mturk.connection.MTurkConnection.get_assignments(),
    returns a string of CSV data representing the assignments.
    '''
    
    import sys, csv, StringIO, json
    
    ## Column names:
    primary_fields = [ 'HITId', 'AssignmentId', 'WorkerId', 'AssignmentStatus', 'AutoApprovalTime', 'AcceptTime', 'SubmitTime', 'ApprovalTime', 'RejectionTime', 'Deadline', 'RequesterFeedback' ]
    qid_fields = set()
    
    ## A sequence of dictionaries mapping field names to values.
    rows = []
    for a in assignments:
        row = {}
        
        for field in primary_fields:
            if hasattr( a, field ):
                row[ field ] = getattr( a, field )
        
        ### Now, question ids.
        ## My older forked boto:
        #answers = [ ( answer.QuestionIdentifier, answer.FreeText ) for answer in a.answers ]
        ## My current forked boto:
        answers = [ ( answer.qid, answer.fields ) for answer in a.answers[0] ]
        
        for qid, fields in answers:
            qid_fields.add( qid )
            
            assert qid not in row
            row[ qid ] = json.dumps( fields )
        
        rows.append( row )
    
    ## Combine the primary column names with the question ids:
    qid_fields = list( qid_fields )
    qid_fields.sort()
    all_fields = primary_fields + qid_fields
    
    out = StringIO.StringIO()
    dw = csv.DictWriter( out, all_fields )
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
        print 'upload_filepaths_to_server() called with zero filepaths.'
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
    print '[ssh "%s" mkdir -p "%s"]' % ( remote_host, remote_dir )
    err = subprocess.Popen( [ 'ssh', remote_host, 'mkdir', '-p', remote_dir ] ).wait()
    assert 0 == err
    
    ## Upload all the files together in the foreground.
    ## NOTE: I think the trailing "/" will cause scp
    ##       to abort with an error code if "remote_dir" is not a directory.
    #print '[scp "%s" "%s"]' % ( filepaths, remote_host + ':' + remote_dir + '/' )
    #err = os.spawnvp( os.P_WAIT, 'scp', [ 'scp' ] + filepaths + [ remote_host + ':' + remote_dir + '/' ] )
    ## UPDATE: Better yet, use rsync.
    print '[rsync "%s" "%s"]' % ( filepaths, remote_host + ':' + remote_dir + '/' )
    err = os.spawnvp( os.P_WAIT, 'rsync', [ 'rsync', '--progress' ] + filepaths + [ remote_host + ':' + remote_dir + '/' ] )
    assert 0 == err


def main():
    import sys, json
    
    def usage():
        print >> sys.stderr, 'Usage:', sys.argv[0], '[really] submit path/to/job.json'
        print >> sys.stderr, 'Usage:', sys.argv[0], '[really] info HITId'
        print >> sys.stderr, 'Usage:', sys.argv[0], '[really] retrieve HITId'
        print >> sys.stderr, 'Usage:', sys.argv[0], '[really] approve AssignmentId [feedback]'
        print >> sys.stderr, 'Usage:', sys.argv[0], '[really] reject AssignmentId [feedback]'
        print >> sys.stderr, 'Usage:', sys.argv[0], '[really] bonus WorkerId AssignmentId dollars feedback'
        print >> sys.stderr, 'Usage:', sys.argv[0], '[really] extend HITId number-of-additional-assignments'
        print >> sys.stderr, 'Usage:', sys.argv[0], '[really] expire HITId'
        print >> sys.stderr, 'Usage:', sys.argv[0], '[really] remove HITId'
        ## TODO:
        #print >> sys.stderr, 'Usage:', sys.argv[0], 'extend HITId additional_assignments ?additional_time?'
        
        print >> sys.stderr, 'Example:', sys.argv[0], 'submit debug.json'
        
        print >> sys.stderr, 'Example "debug.json":'
        print >> sys.stderr, '''{
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
}'''
        
        print >> sys.stderr, 'Note: Commands run in the sandbox unless "really" is present.'
        print >> sys.stderr, 'Note: The "qualifications" field is optional.  The default is to have no qualifications.  Any qualification type supported by boto is allowed.'
        
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
        
        print HITIds2CSV( mturk, [ HITId ] )
    
    def retrieve( argv ):
        if len( argv ) != 1: usage()
        
        HITId = argv[0]
        
        assignments = get_all_assignments_for_HITId( mturk, HITId )
        print assignments2CSV( assignments )
    
    def expire( argv ):
        if len( argv ) != 1: usage()
        
        HITId = argv[0]
        
        response = mturk.expire_hit( HITId )
        print response.status
    
    def remove( argv ):
        if len( argv ) != 1: usage()
        
        HITId = argv[0]
        
        remove_HITId( mturk, HITId )
    
    def approve( argv ):
        if len( argv ) not in (1,2): usage()
        
        AssignmentId = argv.pop(0)
        
        feedback = None if len( argv ) == 0 else argv[0]
        
        response = mturk.approve_assignment( AssignmentId, feedback = feedback )
        print response.status
    
    def reject( argv ):
        if len( argv ) not in (1,2): usage()
        
        AssignmentId = argv.pop(0)
        
        feedback = None if len( argv ) == 0 else argv[0]
        
        response = mturk.reject_assignment( AssignmentId, feedback = feedback )
        print response.status
    
    def extend( argv ):
        if len( argv ) != 2: usage()
        
        HITId, number_of_additional_assignments = argv
        
        try:
            number_of_additional_assignments = int( number_of_additional_assignments )
        except ValueError: usage()
        if number_of_additional_assignments < 0: usage()
        
        print '[extend_hit( %s, %d additional assignments )]' % ( HITId, number_of_additional_assignments )
        response = mturk.extend_hit( HITId, assignments_increment = number_of_additional_assignments )
        print response.status
    
    def bonus( argv ):
        if len( argv ) != 4: usage()
        
        WorkerId, AssignmentId, dollars, feedback = argv
        
        try:
            dollars = float( dollars )
        except ValueError: usage()
        
        price = boto.mturk.price.Price( amount = dollars, currency_code = 'USD' )
        
        response = mturk.grant_bonus( WorkerId, AssignmentId, price, reason = feedback )
        print response.status
    
    def debug( argv ):
        print 'sandbox:', sandbox
    
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
