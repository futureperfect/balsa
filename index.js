var express = require('express'),
    bodyParser = require('body-parser'),
    machina = require('machina'),
    util = require('util'),
    _ = require('lodash'),
    path = require('path'),
    bunyan = require('bunyan');

var app = express();
var log = bunyan.createLogger({name: 'balsa'});

app.use(bodyParser.json());

/*
 * Routes
 */

app.get('/', function (req, res) {
    res.sendFile(path.join(__dirname, 'static/index.html'));
});

app.get('/state', function(req, res) {
    res.json({'timestamp': Math.floor(new Date()), 'volatileState': volatileState, 'persistentState': persistentState});
});

app.put('/appendEntries', function(req, res) {
    stateMachine.appendEntries('{data: 42}');
    res.json(false);
});

app.put('/requestVote', function(req, res, next) {
    var vote = req.body;
    if(!_.has(vote, 'term') || !_.has(vote, 'candidateId') || !_.has(vote, 'lastLogIndex') || !_.has(vote, 'lastLogTerm')) {
        res.status(400).json({ error: 'Invalid vote request body'});
        return;
    }

    if(vote.term < persistentState.currentTerm) {
        log.info('vote denied: stale term');
        res.json({voteGranted: false, term: persistentState.currentTerm});
        return;
    }

    if(vote.term > persistentState.currentTerm) {
        persistentState.currentTerm = vote.term;
    } else if(persistentState.votedFor != "" && persistentState.votedFor != req.candidateId) {
        log.info('vote denied: already voted for', persistentState.votedFor);
        res.json({voteGranted: false, term: persistentState.currentTerm})
        return;
    }

    //TODO: Check whether candidate has an out of date log and deny if so

    //Ok, fine. I'll vote for you.
    persistentState.votedFor = vote.candidateId;
    log.info('vote granted to', persistentState.votedFor, 'in term', persistentState.currentTerm);
    res.json({voteGranted: true, term: persistentState.currentTerm});
});

// Server State

var persistentState = {
    currentTerm: 0,
    votedFor: "",
    log: []
};

var volatileState = {
    commitIndex: 0,
    lastApplied: 0,
    role: "UNKNOWN"
}

// Raft Finite State Machine

var stateMachine = new machina.Fsm({
    initialize: function(options) {

    },
    timeoutFunction: function() {
        return setTimeout( function() {
            this.handle('electionTimeout');
        }.bind(this), 10000); //TODO: Randomize this timeout
    },
    namespace: 'balsa',
    initialState: "uninitialized",
    states: {
        uninitialized: {
            "*": function() {
                this.deferUntilTransition();
                volatileState.role = this.state;
                this.transition('follower');
            }
        },
        follower: {
            _onEnter: function() {
                volatileState.role = this.state;
                this.electionTimer = this.timeoutFunction();
            },
            electionTimeout: 'candidate',
            appendEntries: function(entry) {
                clearTimeout(this.electionTimer);
                log.info(this.state,  '- received entry: ', entry);
                this.electionTimer = this.timeoutFunction();
                this.transition('follower');
            },
            _onExit: function() {
                clearTimeout(this.electionTimer);
            }
        },
        candidate: {
            _onEnter: function() {
                volatileState.role = this.state;
                persistentState.currentTerm++;
                persistentState.votedFor = "";
                //TODO: set a randomized timeout ahead of requesting votes
            },
            appendEntries: function(entry) {
                log.info(this.state,  '- received entry: ', entry);
                this.transition('follower');
            },
            _onExit: function() {
            }
        },
        leader: {
            _onEnter: function() {
                volatileState.role = this.state;
            },
            _onExit: function() {

            }
        }
    },
    reset: function() {
        this.handle('_reset');
    },
    appendEntries: function(entry) {
        this.handle('appendEntries', entry)
    },
    voteRequest: function(data) {
        this.handle('voteRequest', data);
    }
});

stateMachine.on('transition', function (data) {
    log.info(data.fromState, ' -> ', data.toState);
});

var server = app.listen(3000, function () {
    var host = server.address().address;
    var port = server.address().port;
    log.info('Balsa listening at http://%s:%s', host, port);
    stateMachine.reset();
});
