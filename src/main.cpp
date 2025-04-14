#include "MetaService.h"
#include <csignal>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

std::atomic<bool> running{ true };

void signal_handler(int)
{
    running = false;
}

int main(int argc, char* argv[])
{
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // get config
    std::string config_path = "./config/config.json";
    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg.find("--config=") == 0)
        {
            config_path = arg.substr(9);
        }
    }

    try
    {
        std::ifstream config_file(config_path);
        if (!config_file.is_open())
        {
            throw std::runtime_error("Cannot open config file: " + config_path);
        }

        json config;
        config_file >> config;

        // validation
        if (!config.contains("self_ip") || !config.contains("members_ip"))
        {
            throw std::runtime_error("Missing required fields in config");
        }

        MetaService service(config_path, config);

        service.Run();
    }
    catch (const std::exception& e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}