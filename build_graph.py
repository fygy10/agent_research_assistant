from langgraph.graph import START, END, StateGraph
from langgraph.checkpoint.memory import MemorySaver
from models import GeneratedAnalystState
from langgraph.constants import Send
from finalize import ResearchGraphState, write_report, write_introduction, write_conclusion, finalize_report
from converse import InterviewState, generate_questions, search_web, search_wikipedia, generate_answer, save_interview, route_messages, write_section as write_section_func
from langchain_core.messages import HumanMessage


#nest interview graph to run in parallel across all analysts
def build_interview_graph():

    #own state for interviews
    builder = StateGraph(InterviewState)
    
    #nodes with interview functions
    builder.add_node("ask_question", generate_questions)
    builder.add_node("search_web", search_web)
    builder.add_node("search_wikipedia", search_wikipedia)
    builder.add_node("answer_question", generate_answer)
    builder.add_node("save_interview", save_interview)
    builder.add_node("write_section", write_section_func)
    
    #edges through the interview graph
    builder.add_edge(START, "ask_question")
    builder.add_edge("ask_question", "search_web")
    builder.add_edge("search_web", "search_wikipedia")
    builder.add_edge("search_wikipedia", "answer_question")
    builder.add_conditional_edges("answer_question", route_messages, {
        "ask_question": "ask_question",
        "save_interview": "save_interview"
    })
    builder.add_edge("save_interview", "write_section")
    builder.add_edge("write_section", END)
    
    return builder


#begins the interviews with the analystrs
def initiate_all_interviews(state: ResearchGraphState):

    topic = state['topic']

    #allows to be run in parallel + append to main graph state
    return [Send('conduct_interview', {
        'analyst': analyst, 
        'messages': [HumanMessage(content=f'So you said you were writing a report on {topic}?')],
        'max_num_turns': 3, 
        'context': []
    }) for analyst in state['analysts']]



#main graph flow and functionality 
def build_graph(create_analysts_fn):

    #nterview sub-graph
    interview_builder = build_interview_graph()
    
    #main research graph
    builder = StateGraph(ResearchGraphState)
    
    #nodes
    builder.add_node("create_analysts", create_analysts_fn)
    builder.add_node("conduct_interview", interview_builder.compile())  #allows subgraph to be called and returned
    builder.add_node("write_report", write_report)
    builder.add_node("write_introduction", write_introduction)
    builder.add_node("write_conclusion", write_conclusion)
    builder.add_node("finalize_report", finalize_report)

    #edge flow
    builder.add_edge(START, "create_analysts")
    builder.add_conditional_edges("create_analysts", initiate_all_interviews)   #trigger for interviews with analsyst
    builder.add_edge("conduct_interview", "write_report")
    builder.add_edge("conduct_interview", "write_introduction")
    builder.add_edge("conduct_interview", "write_conclusion")
    builder.add_edge(["write_conclusion", "write_report", "write_introduction"], "finalize_report")
    builder.add_edge("finalize_report", END)

    #in-memory saving
    memory = MemorySaver()
    return builder.compile(checkpointer=memory)