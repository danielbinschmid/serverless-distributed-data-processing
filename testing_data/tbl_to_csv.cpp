#include <iostream>
#include <fstream>
#include <string>

int main(int argc, char* argv[])
{
    // Check the number of arguments
    if (argc < 3)
    {
        std::cerr << "Usage: tbl_to_csv <input_file> <output_file>" << std::endl;
        return 1;
    }

    // Open the input and output files
    std::ifstream input(argv[1]);
    std::ofstream output(argv[2]);

    // Read each line of the input file
    std::string line;
    while (std::getline(input, line))
    {
        // Split the line into fields
        std::string field;
        bool first = true;
        for (size_t i = 0; i < line.size(); i++)
        {
            // Check for the field delimiter
            if (line[i] == '|')
            {
                // Write the field to the output file, followed by a comma
                output << field << '|';
                field.clear();
                first = false;
            }
            else
            {
                field += line[i];
            }
        }

        // Write the last field to the output file
        output << field << std::endl;
    }

    // Close the input and output files
    input.close();
    output.close();

    return 0;
}
