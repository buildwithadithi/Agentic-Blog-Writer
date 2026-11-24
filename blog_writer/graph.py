from langgraph.graph import END, START, StateGraph

from blog_writer.nodes.planning import fanout, orchestrator_node
from blog_writer.nodes.research import research_node
from blog_writer.nodes.review import (
    human_review,
    manual_intervention_node,
    revision_node,
    route_after_review,
)
from blog_writer.nodes.router import router_node, route_next
from blog_writer.nodes.wordpress import wordpress_draft_node
from blog_writer.nodes.writing import worker_node
from blog_writer.state import State
from blog_writer.subgraphs.reducer import reducer_node


def build_graph(checkpointer=None):
    graph = StateGraph(State)

    graph.add_node("router", router_node)
    graph.add_node("research", research_node)
    graph.add_node("orchestrator", orchestrator_node)
    graph.add_node("worker", worker_node)
    graph.add_node("reducer", reducer_node)
    graph.add_node("human_review", human_review)
    graph.add_node("revision", revision_node)
    graph.add_node("wordpress_draft", wordpress_draft_node)
    graph.add_node("manual_intervention", manual_intervention_node)

    graph.add_edge(START, "router")
    graph.add_conditional_edges(
        "router",
        route_next,
        {
            "research": "research",
            "orchestrator": "orchestrator",
        },
    )
    graph.add_edge("research", "orchestrator")

    graph.add_conditional_edges("orchestrator", fanout, ["worker"])
    graph.add_edge("worker", "reducer")
    graph.add_edge("reducer", "human_review")

    graph.add_conditional_edges(
        "human_review",
        route_after_review,
        {
            "wordpress_draft_node": "wordpress_draft",
            "revision": "revision",
            "human_review": "human_review",
            "manual_intervention": "manual_intervention",
        },
    )
    graph.add_edge("revision", "human_review")
    graph.add_edge("wordpress_draft", END)
    graph.add_edge("manual_intervention", END)

    return graph.compile(checkpointer=checkpointer)