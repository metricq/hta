#include <hta/hta.hpp>
#include <hta/ostream.hpp>

#include <nlohmann/json.hpp>

#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <mysql_driver.h>

#include <boost/program_options.hpp>
#include <boost/timer/timer.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>

extern "C"
{
#include <signal.h>
}

namespace po = boost::program_options;
using json = nlohmann::json;

volatile sig_atomic_t stop_requested = 0;

void handle_signal(int)
{
    std::cout << "cought sigint, requesting stop.";
    stop_requested = 1;
}

json read_json_from_file(const std::filesystem::path& path)
{
    std::ifstream config_file;
    config_file.exceptions(std::ios::badbit | std::ios::failbit);
    config_file.open(path);
    json config;
    config_file >> config;
    return config;
}

template <typename L>
size_t stream_query(sql::Connection& db, const std::string& query, L cb,
                    size_t chunk_size = 10000000)
{
    std::unique_ptr<sql::PreparedStatement> stmt(db.prepareStatement(query + " LIMIT ?, ?"));
    for (size_t offset = 0;; offset += chunk_size)
    {
        stmt->setUInt64(1, offset);
        stmt->setUInt64(2, chunk_size);
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery());
        while (res->next())
        {
            cb(*res);
        }
        if (res->rowsCount() != chunk_size || stop_requested)
        {
            return offset + res->rowsCount();
        }
    }
}

void import(sql::Connection& in_db, hta::Directory& out_directory, const std::string& metric_name,
            uint64_t min_timestamp, uint64_t max_timestamp)
{
    auto out_metric = out_directory[metric_name];

    std::unique_ptr<sql::Statement> stmt(in_db.createStatement());

    boost::timer::cpu_timer timer;

    std::cout << "Starting import of: " << metric_name << std::endl;

    uint64_t row = 0;
    std::string where;
    if (min_timestamp || max_timestamp)
    {
        where += " WHERE timestamp >= '" + std::to_string(min_timestamp) + "' AND timestamp <= '" +
                 std::to_string(max_timestamp) + "'";
    }
    hta::TimePoint previouse_time;
    auto r =
        stream_query(in_db, "SELECT timestamp, value FROM " + metric_name + where,
                     [&row, &out_metric, &previouse_time](const auto& result) {
                         row++;
                         if (row % 1000000 == 0)
                         {
                             std::cout << row << " rows completed." << std::endl;
                         }
                         hta::TimePoint hta_time{ hta::duration_cast(
                             std::chrono::milliseconds(result.getUInt64(1))) };
                         if (hta_time <= previouse_time)
                         {
                             std::cout << "Skipping non-monotonous timestamp " << hta_time
                                       << std::endl;
                             return;
                         }

                         // Note: Dataheap uses milliseconds. We use nanoseconds.
                         out_metric->insert({ hta_time, static_cast<double>(result.getDouble(2)) });
                     });
    out_metric->flush();
    std::cout << "Imported " << row << " / " << r << " rows for metric: " << metric_name << "\n";
    std::cout << timer.format() << "\n";
}

int main(int argc, char* argv[])
{
    std::string config_file = "config.json";
    std::string metric_name = "dummy";
    std::string out_db_name = "test";
    std::string in_db_name = "import";
    uint64_t min_timestamp = 0;
    uint64_t max_timestamp = 0;

    po::options_description desc("Import dataheap database into HTA.");
    desc.add_options()("help", "produce help message")(
        "config,c", po::value(&config_file), "path to config file (default \"config.json\").")(
        "metric,m", po::value(&metric_name), "name of metric (default \"dummy\").")(
        "database,d", po::value(&out_db_name),
        "name of output database config (default \"test\").")(
        "import,i", po::value(&in_db_name), "name of input database config (default \"import\").")(
        "min-timestamp", po::value(&min_timestamp), "minimal timestamp for dump, in unix-ms")(
        "max-timestamp", po::value(&max_timestamp), "maximal timestamp for dump, in unix-ms");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help"))
    {
        std::cout << desc << "\n";
        return 0;
    }

    auto config = read_json_from_file(std::filesystem::path(config_file));
    hta::Directory out_directory(config);

    // setup input / import database
    sql::Driver* driver = sql::mysql::get_driver_instance();
    const auto& conf_import = config["import"];
    std::string host = conf_import["host"];
    std::string user = conf_import["user"];
    std::string password = conf_import["password"];
    std::string schema = conf_import["schema"];
    std::unique_ptr<sql::Connection> con(driver->connect(host, user, password));
    con->setSchema(schema);

    std::cout.imbue(std::locale(""));
    import(*con, out_directory, metric_name, min_timestamp, max_timestamp);
}
