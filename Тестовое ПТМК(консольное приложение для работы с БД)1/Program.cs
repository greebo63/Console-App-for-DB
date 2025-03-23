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

            //var args = "2_\" sss     s фыв\"_2002-09-18_Female".Split('_', StringSplitOptions.RemoveEmptyEntries);



            string mode = args.Length > 0 ? args[0] : "2";
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
                        try
                        {
                            var employee = new Employee(
                                fullName: args[1],
                                birthdate: args[2],
                                gender: args[3]
                                );
                            await employee.insertIntoTable(connectionString);
                        }
                        catch(Exception ex)
                        {
                            Console.WriteLine($"Произошла ошибка: {ex.Message}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Неверное количество параметров для вставки записи в таблицу.");
                    }
                    break;
                case "3":
                    /*
                     * 3. Вывод всех строк справочника сотрудников, с уникальным значением ФИО+дата, отсортированным по ФИО.
                     *  Вывести ФИО, Дату рождения, пол, кол-во полных лет.
                        Пример запуска приложения:
                        myApp 3
                     */
                    Console.WriteLine("Вы выбрали режим вывода всех строк справочника(пункт 3)");
                    await Employee.selectAllWithUnicFioData(connectionString);
                    break;
                case "4":
                    /*
                     * 4. Заполнение автоматически 1000000 строк справочника сотрудников. 
                     * Распределение пола в них должно быть относительно равномерным, начальной буквы ФИО также. 
                        Добавить заполнение автоматически 100 строк в которых
                         пол мужской и Фамилия начинается с "F".
                        У класса необходимо создать метод, который пакетно отправляет данные в БД, принимая массив объектов.
                        Пример запуска приложения:
                        myApp 4
                     */
                    Console.WriteLine("Вы выбрали режим создания 1000000 записей(пункт 4)");
                    await Employee.insertMillionRecords(connectionString);
                    break;
                default:
                    Console.WriteLine("Вы не выбрали режим работы приложения");
                    break;
                
            }
        }
    }
}
