import os
import json
import csv
import tempfile
import logging
from typing import Any, Sequence, Dict, List
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd

from dotenv import load_dotenv
from mcp.server import Server
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource, Prompt, Resource

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Create server instance
app = Server("tableau-dimension-mapper")

# List of tools and their descriptions for LLM
@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available Tableau dimension mapping tools."""
    return [
        Tool(
            name="remap_dimensions",
            description="""Remaps dimensions in a Tableau workbook based on a CSV mapping file.
            This tool reads both the mapping file and Tableau workbook, applies the mapping rules, and creates a new workbook file.
            For each mapping rule, it replaces all occurrences of the original name with the new name throughout the workbook.
            It returns a report of how many replacements were made and a path to the new workbook file.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "mapping_file_path": {
                        "type": "string",
                        "description": "Path to the CSV mapping file containing the dimension mappings"
                    },
                    "workbook_file_path": {
                        "type": "string",
                        "description": "Path to the Tableau workbook file (.twb) to modify"
                    },
                    "output_file_path": {
                        "type": "string",
                        "description": "Path where the modified workbook file should be saved"
                    }
                },
                "required": ["mapping_file_path", "workbook_file_path", "output_file_path"]
            }
        ),
        Tool(
            name="extract_toml_mappings",
            description="""Analyzes a TOML configuration file to extract dimension mappings and create a CSV file.
            
            Look for a section in the TOML that contains field/dimension mappings, such as [columns.other_renames].
            For each mapping found, extract the original name (key) and the new name (value).
            
            Create a CSV string where each line is in the format: original_name,new_name
            For example, if you find:
            dimension_1 = "Distribution/Program"
            
            The CSV line should be:
            dimension_1,Distribution/Program
            
            Do not include any headers in the CSV.
            Strip any unnecessary quotes from the values.
            Ensure there is no trailing comma or whitespace.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "toml_file_path": {
                        "type": "string",
                        "description": "Path to the TOML configuration file to analyze"
                    },
                    "output_csv_path": {
                        "type": "string",
                        "description": "Path where the CSV mapping file should be saved"
                    }
                },
                "required": ["toml_file_path", "output_csv_path"]
            }
        ),
        Tool(
            name="validate_mapping_file",
            description="""Validates a CSV mapping file to ensure it has the correct format.
            The CSV should have two columns: the first column is the original field name, the second column is the new field name.
            This tool will check if the file is properly formatted and return a list of the mappings it contains.
            
            Example of a valid mapping CSV:
            First name, name First
            Last name, name Last
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "mapping_file_path": {
                        "type": "string",
                        "description": "Path to the CSV mapping file to validate"
                    }
                },
                "required": ["mapping_file_path"]
            }
        ),
        Tool(
            name="validate_tableau_workbook",
            description="""Validates a Tableau workbook file (.twb) to ensure it can be processed.
            This tool checks if the file is a valid XML file with the expected Tableau workbook structure.
            It returns information about the workbook such as version, number of datasources, and worksheets.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "workbook_file_path": {
                        "type": "string",
                        "description": "Path to the Tableau workbook file (.twb) to validate"
                    }
                },
                "required": ["workbook_file_path"]
            }
        ),
        Tool(
            name="analyze_workbook",
            description="""Analyzes a Tableau workbook to identify dimensions and fields that could be remapped.
            This tool parses the workbook XML structure and extracts field names, dimension references, and other metadata.
            It provides a report of the fields found, their usage patterns, and potential candidates for remapping.
            Use this tool to understand the structure of a workbook before creating mapping suggestions.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "workbook_file_path": {
                        "type": "string",
                        "description": "Path to the Tableau workbook file (.twb) to analyze"
                    }
                },
                "required": ["workbook_file_path"]
            }
        ),
        Tool(
            name="write_file",
            description="""Writes content to a file at the specified path.
            This tool is useful for creating CSV mapping files based on your analysis of a Tableau workbook.
            You can use this after analyzing a workbook to create a mapping file with suggested dimension name improvements.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path where the file should be written"
                    },
                    "content": {
                        "type": "string",
                        "description": "Content to write to the file"
                    }
                },
                "required": ["file_path", "content"]
            }
        )
    ]

@app.list_prompts()
async def list_prompts() -> list:
    """List available prompts for Tableau dimension mapper."""
    return [
        {
            "name": "remap_dimensions_from_toml",
            "description": "Remap dimensions in a Tableau workbook using a TOML configuration file",
            "arguments": [
                {
                    "name": "Workbook File Path",
                    "description": "Path to the Tableau workbook file (.twb) to modify",
                    "required": True
                },
                {
                    "name": "Remapping TOML Path",
                    "description": "Path to the TOML file containing dimension mappings",
                    "required": True
                },
                {
                    "name": "Output File Path",
                    "description": "Optional path where the modified workbook should be saved. If not provided, a default path will be generated.",
                    "required": False
                }
            ]
        }
    ]

@app.list_resources()
async def list_resources() -> list[Resource]:
    """List available resources for Tableau dimension mapper.
    
    This server doesn't provide any resources directly, so we return an empty list.
    """
    return []

@app.get_prompt()
async def get_prompt(name: str, arguments: Any) -> dict:
    """Get a prompt to send to the LLM."""
    if name == "remap_dimensions_from_toml":
        # Generate a default output path if not provided
        output_file_path = arguments.get("Output File Path", "")
        if not output_file_path:
            workbook_path = arguments["Workbook File Path"]
            base_name = os.path.basename(workbook_path)
            name_without_ext = os.path.splitext(base_name)[0]
            output_dir = os.path.dirname(workbook_path)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file_path = os.path.join(output_dir, f"{name_without_ext}_remapped_{timestamp}.twb")
            
        prompt_text = f"""
You are a Tableau expert tasked with remapping dimensions in a Tableau workbook using a TOML configuration file.

The user has provided:
- A Tableau workbook file: {arguments["Workbook File Path"]}
- A TOML configuration file: {arguments["Remapping TOML Path"]}

Follow these steps to complete the remapping:

1. First, validate the Tableau workbook using the 'validate_tableau_workbook' tool to ensure it's a valid file.

2. Next, analyze the workbook using the 'analyze_workbook' tool to understand its structure.

3. Extract mappings from the TOML file using the 'extract_toml_mappings' tool. You'll need to:
   - Find the section in the TOML file containing field/dimension mappings (e.g., [columns.other_renames])
   - Extract each original name (key) and new name (value)
   - Create a CSV file with these mappings

4. Save the mappings to a temporary CSV file using the 'write_file' tool.

5. Validate the mapping file using the 'validate_mapping_file' tool to ensure it's properly formatted.

6. Finally, use the 'remap_dimensions' tool to apply the mappings to the workbook and save the result to:
   {output_file_path}

7. Provide a summary of the changes made, including:
   - How many mappings were applied
   - How many replacements were made in the workbook
   - Any potential issues or warnings

Use the tools provided to accomplish this task step by step.
"""
        # Return in the format expected by MCP
        return {
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": prompt_text
                    }
                }
            ]
        }
    
    raise ValueError(f"Unknown prompt: {name}")

@app.call_tool()
async def call_tool(name: str, arguments: Any) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
    """Handle tool calls for Tableau dimension mapping."""
    
    if name == "extract_toml_mappings":
        # Validate required arguments
        required_args = ["toml_file_path", "output_csv_path"]
        if not all(arg in arguments for arg in required_args):
            raise ValueError(f"Missing required arguments. Need: {required_args}")
            
        try:
            # Just read and return the file contents
            with open(arguments["toml_file_path"], 'r') as f:
                return [TextContent(
                    type="text",
                    text=f.read()
                )]
                
        except Exception as e:
            return [TextContent(
                type="text",
                text=f"Error reading TOML file: {str(e)}"
            )]
    
    elif name == "validate_mapping_file":
        # Validate required arguments
        if "mapping_file_path" not in arguments:
            raise ValueError("Missing required argument: mapping_file_path")
        
        mapping_file_path = arguments["mapping_file_path"]
        
        # Check file extension
        if not mapping_file_path.lower().endswith('.csv'):
            return [TextContent(
                type="text",
                text=f"Error: File must be a CSV file. Got {mapping_file_path}"
            )]
        
        try:
            # Read the mapping file
            mappings = []
            with open(mapping_file_path, 'r') as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if len(row) < 2:
                        return [TextContent(
                            type="text",
                            text=f"Error: Line {i+1} does not have at least two columns"
                        )]
                    mappings.append((row[0].strip(), row[1].strip()))
            
            if not mappings:
                return [TextContent(
                    type="text",
                    text="Error: Mapping file is empty"
                )]
            
            # Format the mappings for display
            formatted_mappings = "\n".join([f"• \"{old}\" → \"{new}\"" for old, new in mappings])
            
            return [TextContent(
                type="text",
                text=f"✅ Mapping file is valid with {len(mappings)} mappings:\n\n{formatted_mappings}"
            )]
            
        except Exception as e:
            return [TextContent(
                type="text",
                text=f"Error validating mapping file: {str(e)}"
            )]
            
    elif name == "validate_tableau_workbook":
        # Validate required arguments
        if "workbook_file_path" not in arguments:
            raise ValueError("Missing required argument: workbook_file_path")
        
        workbook_file_path = arguments["workbook_file_path"]
        
        # Check file extension
        if not workbook_file_path.lower().endswith('.twb'):
            return [TextContent(
                type="text",
                text=f"Error: File must be a Tableau workbook (.twb) file. Got {workbook_file_path}"
            )]
        
        try:
            # Read the tableau file and check if it's valid XML
            with open(workbook_file_path, 'r') as f:
                content = f.read()
                soup = BeautifulSoup(content, 'lxml-xml')
                
                # Check if it's a tableau workbook
                if soup.workbook is None:
                    return [TextContent(
                        type="text",
                        text="Error: File does not appear to be a valid Tableau workbook"
                    )]
                
                # Get some basic information about the workbook
                workbook_version = soup.workbook.get('version', 'unknown')
                datasources = len(soup.find_all('datasource'))
                worksheets = len(soup.find_all('worksheet'))
                
                return [TextContent(
                    type="text",
                    text=f"✅ Tableau workbook is valid (version {workbook_version}) with {datasources} datasources and {worksheets} worksheets"
                )]
                
        except Exception as e:
            return [TextContent(
                type="text",
                text=f"Error validating Tableau workbook: {str(e)}"
            )]
            
    elif name == "remap_dimensions":
        # Validate required arguments
        required_args = ["mapping_file_path", "workbook_file_path", "output_file_path"]
        if not all(arg in arguments for arg in required_args):
            raise ValueError(f"Missing required arguments. Need: {required_args}")
        
        mapping_file_path = arguments["mapping_file_path"]
        workbook_file_path = arguments["workbook_file_path"]
        output_file_path = arguments["output_file_path"]
        
        try:
            # Read the mapping file
            mappings = []
            with open(mapping_file_path, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2:
                        mappings.append((row[0].strip(), row[1].strip()))
            
            if not mappings:
                return [TextContent(
                    type="text",
                    text="Error: Mapping file is empty"
                )]
            
            # Read the tableau file
            with open(workbook_file_path, 'r') as f:
                tableau_content = f.read()
            
            # Apply replacements
            modified_content = tableau_content
            replacements_made = 0
            replacements_by_mapping = {}
            
            for old, new in mappings:
                occurrences = modified_content.count(old)
                if occurrences > 0:
                    modified_content = modified_content.replace(old, new)
                    replacements_made += occurrences
                    replacements_by_mapping[old] = occurrences
            
            # Write the modified content to the output file
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            with open(output_file_path, 'w') as f:
                f.write(modified_content)
            
            # Format the replacements for display
            replacements_details = []
            for old, new in mappings:
                count = replacements_by_mapping.get(old, 0)
                replacements_details.append(f"• \"{old}\" → \"{new}\": {count} replacements")
            
            formatted_replacements = "\n".join(replacements_details)
            
            # Use LLM to explain the changes (in a real implementation)
            # Here we just provide a simple explanation based on the data
            explanation = "The dimension remapping has been applied successfully. "
            
            if replacements_made > 0:
                explanation += f"A total of {replacements_made} replacements were made across {len(replacements_by_mapping)} different mappings. "
                explanation += "These changes may affect calculated fields, visualizations, and filters that reference the renamed dimensions. "
                explanation += "Make sure to validate the workbook after opening it in Tableau."
            else:
                explanation += "No replacements were made. This could indicate that the mapping file contains dimension names that don't exist in the workbook."
            
            return [TextContent(
                type="text",
                text=f"✅ Successfully remapped dimensions in the Tableau workbook.\n\n"
                     f"Made {replacements_made} replacements using {len(mappings)} mapping rules.\n\n"
                     f"Replacement details:\n{formatted_replacements}\n\n"
                     f"Analysis of Changes:\n{explanation}\n\n"
                     f"Modified workbook saved to: {output_file_path}"
            )]
            
        except Exception as e:
            return [TextContent(
                type="text",
                text=f"Error remapping dimensions: {str(e)}"
            )]
            
    elif name == "analyze_workbook":
        # Validate required arguments
        if "workbook_file_path" not in arguments:
            raise ValueError("Missing required argument: workbook_file_path")
        
        workbook_file_path = arguments["workbook_file_path"]
        
        try:
            # Read the tableau file
            with open(workbook_file_path, 'r') as f:
                tableau_content = f.read()
                soup = BeautifulSoup(tableau_content, 'lxml-xml')
            
            # Extract column names and metadata
            columns = []
            for column in soup.find_all('column'):
                name = column.get('name', '')
                if name and name not in columns:
                    columns.append(name)
            
            # Extract fields from calculated fields
            calculated_fields = []
            for calculation in soup.find_all('calculation'):
                formula = calculation.get('formula', '')
                if formula:
                    calculated_fields.append(formula)
            
            # Extract worksheet names
            worksheets = []
            for worksheet in soup.find_all('worksheet'):
                name = worksheet.get('name', '')
                if name:
                    worksheets.append(name)
            
            # Format the analysis for display
            analysis = f"# Tableau Workbook Analysis\n\n"
            analysis += f"## Overview\n"
            analysis += f"- **File:** {os.path.basename(workbook_file_path)}\n"
            analysis += f"- **Version:** {soup.workbook.get('version', 'unknown')}\n"
            analysis += f"- **Worksheets:** {len(worksheets)}\n"
            analysis += f"- **Fields:** {len(columns)}\n\n"
            
            analysis += f"## Fields Found\n"
            if columns:
                analysis += "The following fields were found in the workbook:\n\n"
                for column in sorted(columns):
                    analysis += f"- `{column}`\n"
            else:
                analysis += "No fields were found in the workbook.\n"
            
            analysis += f"\n## Worksheets\n"
            if worksheets:
                for worksheet in sorted(worksheets):
                    analysis += f"- {worksheet}\n"
            else:
                analysis += "No worksheets were found in the workbook.\n"
            
            analysis += f"\n## Potential Naming Patterns\n"
            
            # Simple pattern detection
            prefixed_fields = {}
            for column in columns:
                parts = column.split()
                if len(parts) > 1:
                    prefix = parts[0]
                    if prefix in prefixed_fields:
                        prefixed_fields[prefix].append(column)
                    else:
                        prefixed_fields[prefix] = [column]
            
            # Display potential patterns
            if prefixed_fields:
                analysis += "The following potential naming patterns were detected:\n\n"
                for prefix, fields in prefixed_fields.items():
                    if len(fields) > 1:
                        analysis += f"### Fields with prefix '{prefix}':\n"
                        for field in fields:
                            analysis += f"- `{field}`\n"
                        analysis += "\n"
            else:
                analysis += "No clear naming patterns were detected.\n"
            
            return [TextContent(
                type="text",
                text=analysis
            )]
            
        except Exception as e:
            return [TextContent(
                type="text",
                text=f"Error analyzing workbook: {str(e)}"
            )]
            
    elif name == "write_file":
        # Validate required arguments
        required_args = ["file_path", "content"]
        if not all(arg in arguments for arg in required_args):
            raise ValueError(f"Missing required arguments. Need: {required_args}")
        
        file_path = arguments["file_path"]
        content = arguments["content"]
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Write content to file
            with open(file_path, 'w') as f:
                f.write(content)
            
            return [TextContent(
                type="text",
                text=f"✅ Successfully wrote content to file: {file_path}"
            )]
            
        except Exception as e:
            return [TextContent(
                type="text",
                text=f"Error writing to file: {str(e)}"
            )]
    
    else:
        raise ValueError(f"Unknown tool: {name}")

async def main():
    from mcp.server.stdio import stdio_server
    
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main()) 