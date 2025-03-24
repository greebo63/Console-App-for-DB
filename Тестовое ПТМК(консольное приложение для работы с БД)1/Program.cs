using Npgsql;
using System.Configuration;

namespace Тестовое_ПТМК_консольное_приложение_для_работы_с_БД_1
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            string connectionString = ConfigurationManager.ConnectionStrings["pgconn"].ConnectionString;
            Console.WriteLine($"Строка подключения: {connectionString}");


            string mode = args.Length > 0 ? args[0] : "1";
            switch (mode)
            {
                case "1":
                    Console.WriteLine("Вы выбрали режим создания таблицы(пункт 1)");
                    await Employee.createTable(connectionString);
                    break;
                case "2":
                    Console.WriteLine("Вы выбрали режим добавления записи(пункт 2)");
                    if (args.Length == 4)
                    {
                        var employee = new Employee(args);
                        await employee.insertIntoTable(connectionString);
                    }
                    else
                    {
                        Console.WriteLine("Неверное количество параметров для вставки записи в таблицу.");
                    }
                    break;
                case "3":
                    Console.WriteLine("Вы выбрали режим вывода всех строк справочника(пункт 3)");
                    await Employee.selectAllWithUnicFioData(connectionString);
                    break;
                case "4":
                    Console.WriteLine("Вы выбрали режим создания 1000000 записей(пункт 4)");
                    await Employee.insertMillionRecords(connectionString);
                    //await Employee.insertHundredRowsMaleFamiliaStartWithF(connectionString);
                    break;
                case "5":
                    Console.WriteLine("Вы выбрали режим поиска записей по критерию: пол мужской, Фамилия начинается с \"F\".(пункт 5)");
                    await Employee.selectWithConditionMaleAndFamiliaStartWithF(connectionString);
                    break;
                default:
                    Console.WriteLine("Вы не выбрали режим работы приложения");
                    break;
            }
        }
    }
}
