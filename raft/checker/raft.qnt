module raft {
    type Option[a] = Some(a) | None
    type Role = Candidate | Follower | Leader
    type VoteRequest = {term: int, src: str, dst: str}
    type VoteReply = {term: int, granted: bool, src: str, dst: str}
    type Message = RequestVote(VoteRequest) | RequestVoteReply(VoteReply)
    pure val nodes = Set("n1", "n2", "n3")
    var role: str -> Role
    var term: str -> int
    var votedFor: str -> Option[str]
    var votesGranted: str -> Set[str]
    var messages: Set[Message]

    val electionSafety = nodes.forall(x => nodes.forall(y => (term.get(x) == term.get(y) and role.get(x) == Leader and role.get(y) == Leader) implies (x == y))) 

    action init = all {
        role' = nodes.mapBy(x => Follower),
        term' = nodes.mapBy(x => 0),
        votedFor' = nodes.mapBy(x => None),
        votesGranted' = nodes.mapBy(x => Set()),
        messages' = Set()
    }

    action timeout(n) = 
        val newTerm = term.get(n)+1
        all {
        role.get(n) == Follower,
        role' = role.set(n, Candidate),
        term' = term.set(n, newTerm),
        votedFor' = votedFor.set(n, Some(n)),
        votesGranted' = votesGranted.set(n, Set(n)),
        messages' = messages.union(nodes.exclude(Set(n)).map(peer => RequestVote({term: newTerm, src: n, dst: peer})))
    }

    action handleRequestVote(n) = 
        nondet m = oneOf(messages)
        match m {
            | RequestVoteReply(_) => all { false, term' = term, role' = role, votedFor' = votedFor, votesGranted' = votesGranted, messages' = messages }
            | RequestVote(req) =>
                val oldTerm = term.get(n)
                val priorVote = if (req.term > term.get(n)) None else votedFor.get(n)
                val newTerm = if (req.term > oldTerm) req.term else oldTerm
                val granted = req.term >= oldTerm and (priorVote == Some(req.src) or priorVote == None)
                val reply = RequestVoteReply({term: newTerm, granted: granted, src: n, dst: req.src})
                
                all {
                    req.dst == n,
                    term'= term.set(n, newTerm),
                    role' = if (req.term > oldTerm) role.set(n, Follower) else role,
                    votedFor' = votedFor.set(n, if (granted) Some(req.src) else priorVote),
                    votesGranted' = if (req.term > term.get(n)) votesGranted.set(n, Set()) else votesGranted,
                    messages' = messages.exclude(Set(m)).union(Set(reply))
                }
        }

    action handleRequestReply(n) =
        nondet m = oneOf(messages)
        match m {
            | RequestVote => all {false, term' = term, role' = role, votedFor' = votedFor, votesGranted' = votesGranted, messages' = messages }
            | RequestVoteReply(rep) =>
                val oldTerm = term.get(n)
                all {
                    rep.dst == n, 
                    term' = if (rep.term > oldTerm) term.set(n, rep.term) else term, // only set term if response term is higher
                    role' = if (rep.term > oldTerm) role.set(n, Follower) else role, // step down to follower if response's term is higher
                    votedFor' = if (rep.term > oldTerm) votedFor.set(n, None) else votedFor,
                    votesGranted' = if (rep.term > term.get(n)) votesGranted.set(n, Set()) else if(rep.term == oldTerm and rep.granted and role.get(n) == Candidate) votesGranted.set(n, votesGranted.get(n).union(Set(rep.src))) else votesGranted,
                    messages' = messages.exclude(Set(m))
                }
        }

    action becomeLeader(n) = 
        val voteTally = votesGranted.get(n).size()
        all {
            role.get(n) == Candidate,
            voteTally * 2 > nodes.size(),
            role' = role.set(n, Leader),
            term' = term, 
            votedFor' = votedFor, 
            votesGranted' = votesGranted.set(n, Set()), 
            messages' = messages
        }

    action step = 
        nondet node = oneOf(nodes)
        any {
            timeout(node),
            handleRequestVote(node),
            handleRequestReply(node),
            becomeLeader(node)
        }
}