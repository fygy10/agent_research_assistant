from set_env import setup_environment
from analysts import create_analysts
from build_graph import build_graph
import functools
import sys

from langchain_community.tools.tavily_search import TavilySearchResults
import converse
import finalize

#llm initialization 
llm_instance = setup_environment()
converse.llm = llm_instance
finalize.llm = llm_instance

#initialize Tavily search
converse.tavily_search = TavilySearchResults(max_results=3)


#format to present the analysts
def display_analysts(analysts):

    if analysts:
        for analyst in analysts:
            print(f"Name: {analyst.name}")
            print(f"Affiliation: {analyst.affiliation}")
            print(f"Role: {analyst.role}")
            print(f"Description: {analyst.description}")
            print("-" * 50)


#fortmat to display the report
def display_report(report):

    if report:
        print("\n" + "=" * 80)
        print("FINAL REPORT")
        print("=" * 80)
        print(report)
        print("=" * 80)


#run the agentic system
def main():
    try:
        
        #create_analysts function with llm provided with functools.partial
        create_analysts_with_llm = functools.partial(create_analysts, llm=llm_instance)
        
        #starting point of graph input
        graph = build_graph(create_analysts_with_llm)
        
        #input parameters
        number_analysts = 3  
        topic = input("Enter research topic: ")
        
        # Create a new thread - this is important for state management
        thread = {'configurable': {'thread_id': '1'}}
        
        #initial state
        initial_state = {
            'topic': topic, 
            'number_analysts': number_analysts,
            'sections': []  
        }
        
        print("Starting analysis the pipeline for your topic")

        #execute through the graph - identifies the next node and performs the action and output baseed on it
        for event in graph.stream(initial_state, thread, stream_mode="updates"):
            node_name = next(iter(event.keys()))
            print(f"Processing node: {node_name}")
            
            #for each node type, take appropriate action
            if node_name == "create_analysts":
                if 'analysts' in event[node_name]:
                    print("Analysts created:")
                    display_analysts(event[node_name]['analysts'])
            
            elif node_name == "conduct_interview":
                print("  - Conducting interview...")
                if 'analyst' in event[node_name]:
                    print(f"  - Interviewing {event[node_name]['analyst'].name}")
            
            elif node_name == "write_section":
                print("  - Writing section...")
                if 'sections' in event[node_name]:
                    print(f"  - Section created: {event[node_name]['sections'][-1][:100]}...")
            
            elif node_name == "write_report":
                print("  - Writing report...")
            
            elif node_name == "write_introduction":
                print("  - Writing introduction...")
            
            elif node_name == "write_conclusion":
                print("  - Writing conclusion...")
            
            elif node_name == "finalize_report":
                print("  - Finalizing report...")
                if 'final_report' in event[node_name]:
                    print("Final report is ready!")
                    # display_report(event[node_name]['final_report'])

        #get the final state to retrieve the complete report
        print("Retrieving final report...")
        final_state = graph.get_state(thread)
        if 'final_report' in final_state.values:
            report = final_state.values.get('final_report')
            print("Final report retrieved!")
            display_report(report)
        else:
            print("Final report not found in state. Available keys:", final_state.values.keys())
            
            #retrieve sections and build report manually
            if 'sections' in final_state.values:
                print(f"Found {len(final_state.values['sections'])} sections")
                for i, section in enumerate(final_state.values['sections']):
                    print(f"Section {i+1}:")
                    print(section[:500] + "...\n")
    
    #eeor handle
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()