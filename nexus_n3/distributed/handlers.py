"""Legacy message handlers for distributed coordination."""

def handle_register(msg, registry):
    """
    Handle worker registration messages.

    Args:
        msg: Message dict with registration data.
        registry: NodeRegistry instance.

    Returns:
        Response dict if handled, otherwise None.
    """
    if msg["type"] == "REGISTER":
        registry.register_node(msg["node_id"], msg["ip"], role=msg.get("role", "worker"))
        return {"status": "OK"}

def handle_subject_assignment(msg, registry):
    """
    Assign a subject to a node using a simple hash-based strategy.

    Args:
        msg: Message dict with subject assignment request.
        registry: NodeRegistry instance.

    Returns:
        Response dict if handled, otherwise None.
    """
    if msg["type"] == "SUBJECT_ASSIGNMENT_REQUEST":
        subject_id = msg["subject_id"]
        # simple assignment logic
        nodes = list(registry.get_nodes().keys())
        assigned_node = nodes[hash(subject_id) % len(nodes)] if nodes else "master"
        registry.assign_subject(subject_id, assigned_node)
        return {"assigned_node": assigned_node}
