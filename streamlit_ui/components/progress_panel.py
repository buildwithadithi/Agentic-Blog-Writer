import streamlit as st


_NODE_LABELS = {
    "router": "Routing request",
    "research": "Researching topic",
    "orchestrator": "Planning article",
    "worker": "Writing sections",
    "reducer": "Combining content",
    "merge_content": "Combining sections",
    "decide_images": "Planning images",
    "generate_and_place_images": "Generating and placing images",
    "human_review": "Waiting for review",
    "revision": "Revising article",
    "wordpress_draft": "Creating WordPress draft",
    "manual_intervention": "Manual intervention required",
}

_REDUCER_SUBGRAPH_NODES = {
    "merge_content",
    "decide_images",
    "generate_and_place_images",
}


def _node_label(node: str) -> str:
    return _NODE_LABELS.get(node, node.replace("_", " ").capitalize())


def render_progress_panel(
    events: list[dict],
    workflow_status: str,
    error_message: str = "",
    review_status: str = "",
) -> None:
    if not events and workflow_status == "idle":
        return

    active = {}
    completed_stages = []
    completed_sections = {}
    section_total = None

    for event in events:
        event_type = event.get("event")
        node = event.get("node")
        execution_id = event.get("execution_id")

        if event_type == "plan_ready":
            section_total = event.get("section_total")
        elif event_type == "node_started":
            if execution_id is not None:
                active[execution_id] = event
        elif event_type in {
            "node_completed",
            "node_interrupted",
            "node_failed",
        }:
            if execution_id is not None:
                active.pop(execution_id, None)
            if event_type == "node_completed" and node:
                label = _node_label(node)
                if label not in completed_stages:
                    completed_stages.append(label)
            if node == "worker" and event_type == "node_completed":
                section_id = event.get("section_id")
                if section_id is not None:
                    completed_sections[section_id] = (
                        event.get("section_title") or f"Section {section_id}"
                    )

    active_events = list(active.values())
    active_nodes = {event.get("node") for event in active_events}
    if (
        "reducer" in active_nodes
        and active_nodes.intersection(_REDUCER_SUBGRAPH_NODES)
    ):
        active_events = [
            event for event in active_events if event.get("node") != "reducer"
        ]

    st.subheader("Workflow progress")
    if active_events:
        st.markdown("**Current stage**")
        active_workers = [
            event for event in active_events if event.get("node") == "worker"
        ]
        other_stages = [
            event for event in active_events if event.get("node") != "worker"
        ]
        for event in other_stages:
            st.write(_node_label(event["node"]))
        if active_workers:
            st.write(f"Writing {len(active_workers)} section(s) in parallel")
            for event in active_workers:
                if event.get("section_title"):
                    st.write(f"Writing: {event['section_title']}")
    elif workflow_status == "running":
        st.write("Starting workflow...")

    if section_total is not None:
        st.write(
            f"Sections completed: {len(completed_sections)} of {section_total}"
        )
        if completed_sections:
            st.write(
                "Completed sections: "
                + ", ".join(completed_sections.values())
            )

    if completed_stages:
        with st.expander("Completed stages"):
            for stage in completed_stages:
                st.write(stage)

    if workflow_status == "paused_for_review":
        st.info("Your blog is ready for human review.")
    elif workflow_status == "paused":
        st.info("The workflow is paused at an interrupt without review controls.")
    elif workflow_status == "completed":
        if review_status == "manual_intervention_required":
            st.warning(
                "The automatic revision limit was reached; manual intervention is required."
            )
        else:
            st.success("Your blog workflow is complete.")
    elif workflow_status == "error":
        st.error(
            error_message
            or "Blog generation failed. Check the runtime status and try again."
        )
