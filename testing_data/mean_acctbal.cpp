#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <unordered_map>

int main(int argc, char* argv[])
{
    // Check the number of arguments
    if (argc < 2)
    {
        std::cerr << "Usage: mean_column_6 <input_file>" << std::endl;
        return 1;
    }

    // Open the input file
    std::ifstream input(argv[1]);

    // Initialize a map to store the mean values for each value of column 4
    std::unordered_map<std::string, double> mean_values;

    // Initialize a map to store the count of rows for each value of column 4
    std::unordered_map<std::string, int> row_counts;

    // Read each line of the input file
    std::string line;
    while (std::getline(input, line))
    {
        // Split the line into fields
        std::string field;
        int index = 0;
        std::stringstream ss(line);
        std::string column_4;
        double column_6;
        while (std::getline(ss, field, '|'))
        {
            // Check if this is the 4th field
            if (index == 3)
            {
                column_4 = field;
            }
            // Check if this is the 6th field
            else if (index == 5)
            {
                // Convert the value to a double
                std::istringstream(field) >> column_6;
            }
            index++;
        }

        // Update the mean value for this value of column 4
        mean_values[column_4] += column_6;
        row_counts[column_4]++;
    }

    // Close the input file
    input.close();

    
    // Iterate over the map and print the mean values
    for (const auto& pair : mean_values)
    {
        std::cout << "Mean account balance (column 6) value for nation key (column 4) '" << pair.first << "': " << pair.second / row_counts[pair.first] << std::endl;
    }

    return 0;
}
