from models import GeneratedAnalystState
from langgraph.graph import END

# pause point placeholder
def human_feedback(state: GeneratedAnalystState):
    # This will be a pause point in the graph
    # LangGraph will interrupt execution here for human input
    pass

# move to node after feedback
def should_continue(state: GeneratedAnalystState):
    # check if human feedback exists
    human_analyst_feedback = state.get('human_analyst_feedback', None)
    if human_analyst_feedback:
        # Instead of returning to create_analysts, we should return "conduct_interview"
        # to move to the next stage after the analysts are updated
        return "conduct_interview"
    
    # otherwise end
    return END