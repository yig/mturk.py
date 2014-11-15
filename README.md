## About

A convenient command-line tool for using Amazon Mechanical Turk.  
Relies on boto: <https://github.com/boto/boto>

Author: Yotam Gingold <yotam@yotamgingold.com>

Any copyright is dedicated to the Public Domain.
<http://creativecommons.org/publicdomain/zero/1.0/>

## Usage

```
$ ./mturk.py 
Usage: ./mturk.py [really] submit path/to/job.json
Usage: ./mturk.py [really] info HITId
Usage: ./mturk.py [really] retrieve HITId
Usage: ./mturk.py [really] approve AssignmentId [feedback]
Usage: ./mturk.py [really] reject AssignmentId [feedback]
Usage: ./mturk.py [really] bonus WorkerId AssignmentId dollars feedback
Usage: ./mturk.py [really] extend HITId number-of-additional-assignments
Usage: ./mturk.py [really] expire HITId
Usage: ./mturk.py [really] remove HITId
Example: ./mturk.py submit debug.json
Example "debug.json":
{
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
}
Note: Commands run in the sandbox unless "really" is present.
Note: The "qualifications" entry is optional.  The default is to have no qualifications.  Any qualification type supported by boto is supported.
```

## Installation

`git clone --recursive https://github.com/yig/mturk.py.git`

Put your AWS credentials somewhere `boto` can find them (I use a `.boto` file): <http://boto.readthedocs.org/en/latest/boto_config_tut.html>
