import streamlit as st
import sys
import functools
import time
from set_env import setup_environment
from analysts import create_analysts
from build_graph import build_graph
from langchain_community.tools.tavily_search import TavilySearchResults
import converse
import finalize
import pdfkit
from io import BytesIO
import markdown
import weasyprint


#page configuration
st.set_page_config(
    page_title="LangGraph Research Assistant",
    page_icon="📊",
    layout="wide"
)



#initialize session state variables with default values for streamlit persistent memory
#streamlit memory exists while server is running
if 'graph' not in st.session_state:
    st.session_state.graph = None
if 'thread' not in st.session_state:
    st.session_state.thread = {'configurable': {'thread_id': '1'}}
if 'llm' not in st.session_state:
    st.session_state.llm = None
if 'analysts' not in st.session_state:
    st.session_state.analysts = None
if 'final_report' not in st.session_state:
    st.session_state.final_report = None
if 'progress' not in st.session_state:
    st.session_state.progress = 0
if 'current_step' not in st.session_state:
    st.session_state.current_step = ""
if 'debug_log' not in st.session_state:
    st.session_state.debug_log = []



#logging
def log_to_debug(message):

    timestamp = time.strftime("%H:%M:%S")
    st.session_state.debug_log.append(f"[{timestamp}] {message}")



#initialize setup environment
def setup_environment_st():

    try:
        with st.spinner("Setting up environment..."):

            #llm
            llm_instance = setup_environment()
            st.session_state.llm = llm_instance
            
            #modules
            converse.llm = llm_instance
            finalize.llm = llm_instance
            
            #Tavily search e
            try:
                converse.tavily_search = TavilySearchResults(max_results=3)
                log_to_debug("Tavily search setup successful")
            except Exception as e:
                log_to_debug(f"Tavily search setup failed: {str(e)}")
                st.warning("Tavily search setup failed. Some features may be limited.")
                

            return True
    except Exception as e:
        st.error(f"Failed to setup environment: {str(e)}")
        log_to_debug(f"Environment setup error: {str(e)}")
        return False



#adapted create_analysts function and starting point for graph input
def init_graph():

    try:
        #function with llm already provided
        create_analysts_with_llm = functools.partial(create_analysts, llm=st.session_state.llm)
        
        #build the graph
        st.session_state.graph = build_graph(create_analysts_with_llm)
        log_to_debug("Graph initialized successfully")
        return True
    except Exception as e:
        st.error(f"Failed to initialize graph: {str(e)}")
        log_to_debug(f"Graph initialization error: {str(e)}")
        return False


#execute and listener during graph execution
def run_pipeline(topic, num_analysts):

    try:

        #progress state (reset)
        st.session_state.progress = 0
        st.session_state.current_step = "Starting..."
        st.session_state.analysts = None
        st.session_state.final_report = None
        
        #progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        #initial state for the graph
        initial_state = {
            'topic': topic, 
            'number_analysts': num_analysts,
            'sections': []
        }
        

        #graph nodes and completed nodes
        total_steps = 7  # create_analysts, conduct_interviews, write_sections, write_report, write_intro, write_conclusion, finalize
        step_count = 0
        

        #status update - create analysts
        status_text.text("Creating analyst personas...")
        st.session_state.current_step = "Creating analyst personas"
        

        #run the grapha nd returnt eh stream of event
        #execute through the graph - identifies the next node and performs the action and output baseed on it
        for event in st.session_state.graph.stream(initial_state, st.session_state.thread, stream_mode="updates"):
            node_name = next(iter(event.keys()))
            log_to_debug(f"Processing node: {node_name}")
            
            #update progress based on the current node
            #for each node type, take appropriate action
            if node_name == "create_analysts":
                if 'analysts' in event[node_name]:
                    st.session_state.analysts = event[node_name]['analysts']
                    step_count += 1
                    st.session_state.progress = step_count / total_steps
                    progress_bar.progress(st.session_state.progress)
                    status_text.text("Conducting interviews with analysts...")
                    st.session_state.current_step = "Conducting interviews"
            
            elif node_name == "conduct_interview":
                if 'section' in event[node_name] and event[node_name]['section']:
                    step_count += 1
                    st.session_state.progress = min(step_count / total_steps, 0.6)  #60% until all interviews complete
                    progress_bar.progress(st.session_state.progress)
            
            elif node_name == "write_report":
                step_count += 1
                st.session_state.progress = step_count / total_steps
                progress_bar.progress(st.session_state.progress)
                status_text.text("Writing main report content...")
                st.session_state.current_step = "Writing report"
            
            elif node_name == "write_introduction":
                step_count += 1
                st.session_state.progress = step_count / total_steps
                progress_bar.progress(st.session_state.progress)
                status_text.text("Writing introduction...")
                st.session_state.current_step = "Writing introduction"
            
            elif node_name == "write_conclusion":
                step_count += 1
                st.session_state.progress = step_count / total_steps
                progress_bar.progress(st.session_state.progress)
                status_text.text("Writing conclusion...")
                st.session_state.current_step = "Writing conclusion"
            
            elif node_name == "finalize_report":
                if 'final_report' in event[node_name]:
                    st.session_state.final_report = event[node_name]['final_report']
                    step_count += 1
                    st.session_state.progress = 1.0
                    progress_bar.progress(st.session_state.progress)
                    status_text.text("Research completed!")
                    st.session_state.current_step = "Complete"
        

        #verify progress complete
        if st.session_state.progress < 1.0:
            st.session_state.progress = 1.0
            progress_bar.progress(1.0)
            status_text.text("Research completed!")
            st.session_state.current_step = "Complete"
        

        # Get the final state to ensure we have the report
        if not st.session_state.final_report:
            final_state = st.session_state.graph.get_state(st.session_state.thread)
            if 'final_report' in final_state.values:
                st.session_state.final_report = final_state.values.get('final_report')
        

        return True
        

    except Exception as e:
        st.error(f"Error in research pipeline: {str(e)}")
        log_to_debug(f"Pipeline error: {str(e)}")
        import traceback
        log_to_debug(traceback.format_exc())
        return False



#run application
def main():
    st.title("🐟 Silverside Research Assistant")
    st.write("Generate comprehensive research reports with AI expert analysts!")
    
    #sidebar setup and configuration
    with st.sidebar:
        st.header("Configuration")
        
        #environment setup button
        if st.button("Setup Environment"):
            if setup_environment_st():
                st.success("Environment setup completed!")
                init_graph()
            else:
                st.error("Environment setup failed.")
        
        #setup status
        if st.session_state.llm is not None:
            st.success("✅ LLM is configured")
        else:
            st.warning("⚠️ LLM not configured")
            
        if st.session_state.graph is not None:
            st.success("✅ Graph is initialized")
        else:
            st.warning("⚠️ Graph not initialized")
        
        #debug section
        with st.expander("Debug Log"):
            for log_entry in st.session_state.debug_log:
                st.text(log_entry)
    

    #user directions
    if st.session_state.llm is None:
        st.info("Please set up the environment using the button in the sidebar to get started.")
        return
        

    #user input for research topic
    st.header("Research Topic")
    topic = st.text_input("Enter the research topic you want to analyze:", 
                           placeholder="Example: The impact of AI on healthcare")
    

    #number of analysts slide bar
    num_analysts = st.slider("Number of analyst personas:", min_value=2, max_value=5, value=3)
    

    #run button
    if st.button("Generate Research Report", type="primary", disabled=(not topic)):
        if not topic:
            st.warning("Please enter a research topic.")
        else:
            with st.spinner(f"Running research pipeline on '{topic}'..."):
                if run_pipeline(topic, num_analysts):
                    st.success("Research complete!")
                else:
                    st.error("Research pipeline failed. Check the debug log for details.")
    

    #progress if process is running
    if st.session_state.progress > 0 and st.session_state.progress < 1.0:
        st.progress(st.session_state.progress)
        st.info(f"Current step: {st.session_state.current_step}")
    

    #display analysts if available
    if st.session_state.analysts:
        with st.expander("Analyst Personas", expanded=st.session_state.final_report is None):
            for analyst in st.session_state.analysts:
                with st.container():
                    st.subheader(analyst.name)
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Role:** {analyst.role}")
                        st.write(f"**Affiliation:** {analyst.affiliation}")
                    with col2:
                        st.write(f"**Description:** {analyst.description}")
                    st.divider()
    

    #display final report if available
    if st.session_state.final_report:
        st.header("Research Report")
        st.markdown(st.session_state.final_report)
        
        # Add download buttons
        col1, col2 = st.columns(2)
        
        with col1:
            # Markdown download (this works with your current code)
            st.download_button(
                label="Download as Markdown",
                data=st.session_state.final_report,
                file_name=f"research_report_{topic.replace(' ', '_')}.md",
                mime="text/markdown"
            )
        
        with col2:
            try:
                # Convert markdown to HTML
                html = markdown.markdown(st.session_state.final_report)
                
                # Convert HTML to PDF
                pdf_bytes = weasyprint.HTML(string=html).write_pdf()
                
                # PDF download button
                st.download_button(
                    label="Download as PDF",
                    data=pdf_bytes,
                    file_name=f"research_report_{topic.replace(' ', '_')}.pdf",
                    mime="application/pdf"
                )
            except Exception as e:
                st.warning(f"PDF generation failed: {str(e)}")
                

if __name__ == "__main__":
    main()