# Skylark_Assignment_Vamshik_M
Github Repo for the assignment by Skylark Drones
This project implements an AI-powered Business Intelligence agent that answers founder-level questions using live data from monday.com boards.

The agent retrieves live data from Deals and Work Orders boards, cleans and normalizes messy business data, routes user queries to analytics functions using an LLM, and returns structured insights along with an executive summary.

## Live Demo

Streamlit App:
" "
Monday.com Boards:
" "

## Features

- Live monday.com API integration
- Handles messy business data with automated cleaning
- LLM-powered query understanding
- Cross-board analytics (Deals + Work Orders)
- Executive summaries generated for leadership
- Visible agent trace showing system actions
- Graceful handling of missing or incomplete data

## Example Questions

You can ask questions like:

- Show pipeline value by sector
- What is the expected pipeline value?
- Show the top deals in the pipeline
- What is the total executed revenue?
- Show sector performance
- Find projects related to proof of concept

## Architecture

User Query
    ↓
Streamlit Interface
    ↓
Live monday.com API Fetch
    ↓
Data Cleaning Layer
    ↓
LLM Query Routing
    ↓
Analytics Functions
    ↓
Executive Summary Generation
    ↓
UI Output + Agent Trace

## Tech Stack

- Python
- Streamlit
- Pandas
- Monday.com GraphQL API
- Groq (LLM inference)