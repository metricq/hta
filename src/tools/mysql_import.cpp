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

#include <cassert>
#include <cmath>

extern "C"
{
#include <signal.h>
}

namespace po = boost::program_options;
using json = nlohmann::json;

volatile sig_atomic_t stop_requested = 0;

void handle_signal(int)
{
    std::cout << "caught sigint, requesting stop." << std::endl;
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

void import(sql::Connection& in_db, hta::Directory& out_directory,
            const std::string& out_metric_name, const std::string& in_metric_name,
            uint64_t min_timestamp, uint64_t max_timestamp)
{
    auto out_metric = out_directory[out_metric_name];

    std::unique_ptr<sql::Statement> stmt(in_db.createStatement());

    boost::timer::cpu_timer timer;

    std::cout << "Starting import of: " << in_metric_name << std::endl;

    uint64_t row = 0;
    std::string where;
    if (min_timestamp || max_timestamp)
    {
        where += " WHERE timestamp >= '" + std::to_string(min_timestamp) + "' AND timestamp <= '" +
                 std::to_string(max_timestamp) + "'";
    }
    hta::TimePoint previous_time;
    auto r =
        stream_query(in_db, "SELECT timestamp, value FROM " + in_metric_name + where,
                     [&row, &out_metric, &previous_time](const auto& result) {
                         row++;
                         if (row % 1000000 == 0)
                         {
                             std::cout << row << " rows completed." << std::endl;
                         }
                         hta::TimePoint hta_time{ hta::duration_cast(
                             std::chrono::milliseconds(result.getUInt64(1))) };
                         if (hta_time <= previous_time)
                         {
                             std::cout << "Skipping non-monotonous timestamp " << hta_time
                                       << std::endl;
                             return;
                         }
                         previous_time = hta_time;
                         // Note: Dataheap uses milliseconds. We use nanoseconds.
                         out_metric->insert({ hta_time, static_cast<double>(result.getDouble(2)) });
                     });
    out_metric->flush();
    std::cout << "Imported " << row << " / " << r << " rows for metric: " << out_metric_name
              << "\n";
    std::cout << timer.format() << "\n";
}

void select_interval(sql::Connection& con, json& metric_config, const std::string& in_metric_name)
{
    auto stmt = std::unique_ptr<sql::PreparedStatement>(con.prepareStatement(
        std::string("SELECT COUNT(`timestamp`), MIN(`timestamp`), MAX(`timestamp`) FROM ") +
        in_metric_name));
    auto result = std::unique_ptr<sql::ResultSet>(stmt->executeQuery());
    assert(result->next());
    auto count = result->getUInt64(1);
    auto db_min_timestamp = result->getInt64(2);
    auto db_max_timestamp = result->getInt64(3);

    assert(count > 1);

    auto average_interval_ns = (db_max_timestamp - db_min_timestamp) * 1e6 / (count - 1);
    int interval_factor = 10;
    if (metric_config.count("interval_factor"))
    {
        interval_factor = metric_config["interval_factor"];
    }
    int interval_min = std::lround(std::pow(
        interval_factor, std::ceil(std::log(average_interval_ns) / std::log(interval_factor)) + 1));
    metric_config["interval_min"] = interval_min;

    std::cout << "Total count: " << count << "\n";
    std::cout << "Timestamp range: " <<  db_min_timestamp << " - " << db_max_timestamp << "\n";
    std::cout << "Average interval " << average_interval_ns << "ns.\n";
    std::cout << "Interval factor: " << interval_factor << "\n";
    std::cout << "Determined interval_min of " << interval_min << "\n";
}

int main(int argc, char* argv[])
{
    std::string config_file = "config.json";
    std::string metric_name = "dummy";
    std::string out_db_name = "test";
    std::string in_db_name = "import";
    uint64_t min_timestamp = 0;
    uint64_t max_timestamp = 0;
    bool auto_interval = false;

    po::options_description desc("Import dataheap database into HTA.");
    desc.add_options()("help", "produce help message")(
        "config,c", po::value(&config_file), "path to config file (default \"config.json\").")(
        "metric,m", po::value(&metric_name), "name of metric (default \"dummy\").")(
        "database,d", po::value(&out_db_name),
        "name of output database config (default \"test\").")(
        "import,i", po::value(&in_db_name), "name of input database config (default \"import\").")(
        "min-timestamp", po::value(&min_timestamp), "minimal timestamp for dump, in unix-ms")(
        "max-timestamp", po::value(&max_timestamp), "maximal timestamp for dump, in unix-ms")(
        "auto-interval", po::bool_switch(&auto_interval), "automatically select an interval");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help"))
    {
        std::cout << desc << "\n";
        return 0;
    };

    std::string in_metric_name = metric_name;
    std::string out_metric_name = metric_name;
    std::replace(in_metric_name.begin(), in_metric_name.end(), '.', '_');
    std::replace(out_metric_name.begin(), out_metric_name.end(), '_', '.');

    auto config = read_json_from_file(std::filesystem::path(config_file));

    // setup input / import database
    sql::Driver* driver = sql::mysql::get_driver_instance();
    const auto& conf_import = config["import"];
    std::string host = conf_import["host"];
    std::string user = conf_import["user"];
    std::string password = conf_import["password"];
    std::string schema = conf_import["schema"];
    std::unique_ptr<sql::Connection> con(driver->connect(host, user, password));
    con->setSchema(schema);

    if (auto_interval)
    {
        select_interval(*con, config[out_metric_name], in_metric_name);
    }

    hta::Directory out_directory(config);

    std::cout.imbue(std::locale(""));
    signal(SIGINT, handle_signal);
    import(*con, out_directory, in_metric_name, out_metric_name, min_timestamp, max_timestamp);
}
