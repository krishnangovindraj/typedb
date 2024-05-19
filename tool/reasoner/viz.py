import re
import networkx as nx
from matplotlib import pyplot as plt

class Viz:
    PORT_REGEX = re.compile(r"PORT\: Node\[([0-9]+)\] opened a port to Node\[([0-9]+)\]")
    CANDIDACY_REGEX = re.compile(r"SEND_RESPONSE: Node\[([0-9]+)\] sent response Candidacy\[([0-9]+)\] to Node\[([0-9]+)\]")
    DONE_REGEX = re.compile(r"SEND_RESPONSE: Node\[([0-9]+)\] sent response Done\[([0-9]+)\] to Node\[([0-9]+)\]")
    TERMINATE_REGEX = re.compile(r"TERMINATE: Node\[([0-9]+)\] has terminated")
    GROWTREE_REGEX = re.compile(r"RECV_REQUEST: Node\[([0-9]+)\] received request GrowTree\[([0-9]+): ([0-9]+)\] from Node\[([0-9]+)\]")
    TREEVOTE_REGEX = re.compile(r"SEND_RESPONSE: Node\[([0-9]+)\] sent response TreeVote\[([0-9]+): ([0-9]+)/([0-9]+)\] to Node\[([0-9]+)\]")
    
    def __init__(self):
        self.G = {}
        self.candidacy = {}
        self.done = {}
        self.growtree = {}
        self.treevote = {}
    
    def process_line(self, line):
        port_match = Viz.PORT_REGEX.match(line)
        if port_match is not None:
            u,v = tuple(int(g) for g in port_match.groups())
            self.add_edge(u, v)

        candidacy_match = Viz.CANDIDACY_REGEX.match(line)
        if candidacy_match is not None:
            u, candidate, v = tuple(int(g) for g in candidacy_match.groups())
            self.candidacy[u] = candidate

        growtree_match = Viz.GROWTREE_REGEX.match(line)
        if growtree_match is not None:
            u, root, target, ancestor = tuple(int(g) for g in growtree_match.groups())
            self.growtree[u] = "[%d: %d @ %d]"%(root, target, ancestor)

        treevote_match = Viz.TREEVOTE_REGEX.match(line)
        if treevote_match is not None:
            u, root, in_target, out_target, ancestor = tuple(int(g) for g in treevote_match.groups())
            self.treevote[u] = "[%d: %d/%d @ %d]"%(root, in_target, out_target, ancestor)


        done_match = Viz.DONE_REGEX.match(line)
        if done_match is not None:
            u, _ans, _v = tuple(int(g) for g in done_match.groups())
            self.done[u] = True
        
        terminate_match = Viz.TERMINATE_REGEX.match(line)
        if terminate_match is not None:
            u = tuple(int(g) for g in terminate_match.groups())
            self.done[u] = True

        

    def add_edge(self, u, v):
        if u not in self.G:
            self.G[u] = list()
        self.G[u].append(v)

    def all_nodes(self):
        nodes = list(set([k for k in self.G.keys()] + [v for V in self.G.values() for v in V]))
        sorted(nodes)
        return nodes

    def all_edges(self):
        edges = [(k,v) for k in self.G for v in self.G[k]]
        sorted(edges)
        return edges

    def get_props(self, n):
        return\
        "[%s]"%(self.candidacy.get(n, "-")) + "\n" + \
        self.growtree.get(n, "-") + "\n" + \
        self.treevote.get(n, "-") + "\n"


    def render_graphviz(self):
        import graphviz
        dot = graphviz.Digraph('actors')
        for n in self.all_nodes():
            props = self.get_props(n)
            label = str(n) + "\n" + props
            attr = {"style": "filled", "color" : "lightblue"}
            if n in self.done: attr["color"] = "grey"
            dot.node(str(n), label, _attributes=attr)
            

        for u in self.G:
            for v in self.G[u]:
                dot.edge(str(u), str(v))
        dot.render(directory='graphviz_out', view=True)

    def render_networkx(self):
        import networkx
        nxg = networkx.DiGraph()
        for n in self.all_nodes():
            nxg.add_node(n)

        for (u,v) in self.all_edges():
            nxg.add_edge(u, v)
        pos = networkx.nx_pydot.pydot_layout(nxg)
        networkx.draw(nxg, pos, with_labels=True)
        plt.show()
    
    
def main():
    from sys import argv
    viz = Viz()
    for l in open(argv[1]):
        viz.process_line(l)
    
    print(viz.all_nodes())
    print(viz.all_edges())
    viz.render_graphviz()
    # viz.render_networkx()
    

if __name__ == "__main__" : main()
            