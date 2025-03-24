# Tableau Dimension Mapper

An MCP Server that allows you to remap dimensions in Tableau workbooks based on mappings from a CSV file.

## Features

- **Validate CSV Mapping Files**: Ensure your mapping files are correctly formatted
- **Validate Tableau Workbooks**: Check that your Tableau workbooks can be processed
- **Analyze Workbooks**: Identify dimensions and fields that could be remapped
- **Suggest Mappings**: Get intelligent suggestions for improving field names
- **Remap Dimensions**: Apply mappings to create a new Tableau workbook with remapped dimensions

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/tableau-dimension-mapper.git
```

## Usage

The Tableau Dimension Mapper is designed to be used as an MCP server within the Claude desktop app. MCP (Model-Centric Protocol) is a protocol for AI assistants to control tools.

### Setting up in Claude Desktop App

Add the following configuration to your Claude desktop app configuration:

```json
"mcpServers": {
  "tableau-mapper": {
    "command": "uv",
    "args": [
      "--directory",
      "/path/to/cloned/Tableau",
      "run",
      "tableau-dimension-mapper"
    ]
  }
}
```

Note: You need to adjust the directory path to match where you cloned the repository.

### Tools Available

1. **validate_mapping_file**: Validates a CSV mapping file
   - Required parameters:
     - `mapping_file_path`: Path to the CSV mapping file

2. **validate_tableau_workbook**: Validates a Tableau workbook file
   - Required parameters:
     - `workbook_file_path`: Path to the Tableau workbook file (.twb)

3. **analyze_workbook**: Analyzes a Tableau workbook to identify dimensions
   - Required parameters:
     - `workbook_file_path`: Path to the Tableau workbook file (.twb)

4. **suggest_mappings**: Suggests potential dimension mappings
   - Required parameters:
     - `workbook_file_path`: Path to the Tableau workbook file (.twb)
     - `output_file_path`: Path where the suggested mappings CSV should be saved

5. **remap_dimensions**: Remaps dimensions in a Tableau workbook
   - Required parameters:
     - `mapping_file_path`: Path to the CSV mapping file
     - `workbook_file_path`: Path to the Tableau workbook file (.twb)
     - `output_file_path`: Path where the modified workbook should be saved

### Mapping File Format

The mapping file should be a CSV file with two columns:
- First column: Original field name
- Second column: New field name

Example:
```
First name, name First
Last name, name Last
```

## License

MIT 