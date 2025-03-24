using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using static System.Runtime.InteropServices.JavaScript.JSType;
using Faker;
using RandomDataGenerator;
using System.Diagnostics;

namespace Тестовое_ПТМК_консольное_приложение_для_работы_с_БД_1
{
    class Employee
    {
        enum Gender
        {
            Male,
            Female
        }
        //переменные
        private string _fullName = "";
        private DateOnly _birthdate = new();
        private string _gender = "";



        //свойства
        public string fullName
        {
            get
            {
                return _fullName;
            }
            set
            {
                try
                {
                    var value_split = value.Split(" ", StringSplitOptions.RemoveEmptyEntries);
                    if (value_split.Length>=2)
                    {
                        _fullName = string.Join(' ', value_split);
                    }
                    else
                    {
                        throw new ArgumentException("В ФИО должно быть передано как минимум имя и фамилия");
                    }
                }
                catch(ArgumentException ex)
                {
                    Console.WriteLine($"Произошла ошибка: {ex.Message}");
                    Environment.Exit(1);
                }
            }
        }
        public string birthdate
        {
            get
            {
                return _birthdate.ToString();
            }
            set
            {
                try
                {
                    DateOnly value_to_date = new DateOnly();
                    if (DateOnly.TryParse(value, out value_to_date) && value.Length == 10)
                    {
                        _birthdate = value_to_date;
                    }
                    else
                    {
                        throw new ArgumentException("Неправильная дата рождения");
                    }
                }
                catch (ArgumentException ex)
                {
                    Console.WriteLine($"Произошла ошибка: {ex.Message}");
                    Environment.Exit(1);
                }
            }
        }

        public DateOnly birthdate_date_only // дата нужна и в строковом формате, и в формате даты, поэтому создано 2 свойства для работы с 1 переменной
        {
            get
            {
                return _birthdate;
            }
        }
        public string gender
        {
            get
            {
                return _gender;
            }
            set
            {
                try
                {
                    if (value == "Male" || value == "Female")
                    {
                        _gender = value;   
                    }
                    else
                    {
                        throw new Exception("Неправильный пол человека");
                    }
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"Произошла ошибка: {ex.Message}");
                    Environment.Exit(1);
                }
            }
        }

        //конструкторы
        public Employee(string fullName, string birthdate, string gender)
        {
            this.fullName = fullName;
            this.birthdate = birthdate;
            this.gender = gender;
        }

        public Employee(string[] args)
        {
            this.fullName = args[1];
            this.birthdate = args[2];
            this.gender = args[3];
        }

        //методы

        //пункт 1
        static public async Task createTable(string connectionString)
        {
            try
            {
                string createTableQuery = @"
                    CREATE TABLE test.Employee(
                        id serial primary key,
                        fio varchar(255),
                        birthdate date,
                        gender varchar(6)
                    )";

                await using var dataSource = NpgsqlDataSource.Create(connectionString);
                await using var connection = await dataSource.OpenConnectionAsync();


                await using var command = dataSource.CreateCommand(createTableQuery);
                await command.ExecuteNonQueryAsync();

                Console.WriteLine("Таблица успешно создана.");
                
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Произошла ошибка: {ex.Message}");
                Environment.Exit(1);
            }
            
        }

        //пункт 2
        public int ageCounter() //считает возраст экземпляра
        {
            var year_dif = DateTime.Now.Year - _birthdate.Year;
            if (DateTime.Now > _birthdate.ToDateTime(TimeOnly.MinValue).AddYears(year_dif)){
                return DateTime.Now.Year - _birthdate.Year;
            }
            else
            {
                return DateTime.Now.Year - _birthdate.Year - 1;
            }
        }

        static public int ageCounter(string date) //считает возраст по заданной дате рождения
        {
            DateOnly date_dateOnly = new DateOnly();
            if (DateOnly.TryParseExact(date, "dd.MM.yyyy", out date_dateOnly))
            {
                var year_dif = DateTime.Now.Year - date_dateOnly.Year;
                if (DateTime.Now > date_dateOnly.ToDateTime(TimeOnly.MinValue).AddYears(year_dif))
                {
                    return DateTime.Now.Year - date_dateOnly.Year;
                }
                else
                {
                    return DateTime.Now.Year - date_dateOnly.Year - 1;
                }
            }
            return -1;
        }

        public async Task insertIntoTable(string connectionString)
        {
            await this.batchInsertExecutor(connectionString);
        }

        private async Task batchInsertExecutor(string connectionString)
        {
            try
            {
                string insertQuery = $@"INSERT INTO test.employee(fio, birthdate, gender)
                                    VALUES ($1, $2, $3)";

                await using var dataSource = NpgsqlDataSource.Create(connectionString);
                await using var connection = await dataSource.OpenConnectionAsync();

                await using var batch = new NpgsqlBatch(connection);

                var command = new NpgsqlBatchCommand(insertQuery);
                command.Parameters.AddWithValue(this.fullName);
                command.Parameters.AddWithValue(NpgsqlDbType.Date, this.birthdate_date_only);
                command.Parameters.AddWithValue(this.gender);

                batch.BatchCommands.Add(command);

                await batch.ExecuteNonQueryAsync();
            }
            catch (Exception ex) 
            {
                Console.WriteLine(ex.Message);
                Environment.Exit(1);
            }
            
        }

        //пункт 3
        static public async Task selectAllWithUnicFioData(string connectionString)
        {
            string selectAllWithUnicFioDataQuery = $@"
                WITH unic_fio_date AS(
	                SELECT 
		                min(id) id,
		                fio,
		                birthdate
	                FROM test.employee e 
	                GROUP BY fio, birthdate
                )

                SELECT u.fio, u.birthdate, e.gender 
                FROM unic_fio_date u
	                INNER JOIN test.employee e 
		                ON u.id=e.id
                ORDER BY fio";

            await Employee.selectExecutor(connectionString, selectAllWithUnicFioDataQuery, 3);
        }

        static private async Task selectExecutor(string connectionString, string selectQuery, int mode)
        {
            try
            {
                if (mode == 3)
                {
                    await using var dataSource = NpgsqlDataSource.Create(connectionString);
                    await using var connection = await dataSource.OpenConnectionAsync();

                    await using var command = new NpgsqlCommand(selectQuery, connection);
                    await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
                    string datebirth;
                    while (await reader.ReadAsync())
                    {
                        datebirth = reader["birthdate"].ToString().Substring(0, 10);
                        Console.WriteLine($@"{reader["fio"]} {datebirth} {reader["gender"]} {Employee.ageCounter(datebirth)}");
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                Environment.Exit(1);
            }
        }

        //пункт 4
        static public async Task insertMillionRecords(string connectionString)
        {
            for (int k = 0; k < 100; k++) //отправка с помощью 100 бачей выбрана в связи с тем, что за раз миллион записей долго вставляются
            {
                Employee[] tenThousandRecords = Employee.createRecords(10000);

                await Employee.batchInsertExecutor(connectionString, tenThousandRecords);
            }
        }
        static private Employee[] createRecords(int length)
        {
            Employee[] res = new Employee[length];
            try
            {
                int counter = 0; //счетчик для равномерного распределения начальных букв у фамилий
                char[] alphabet = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };


                DateTime startDate = DateTime.MinValue.AddYears(1900);
                Random gen = new Random();
                int range = (DateTime.Today - startDate).Days; //промежуток, с помощью которого будет генерироваться случайная дата рождения

                for (int i = 0; i < res.Length; i++)
                {
                    if (counter == alphabet.Length)
                    {
                        counter = 0;
                    }
                    string familia = Faker.Name.Last();
                    familia = alphabet[counter].ToString() + familia.Substring(1);
                    res[i] = new Employee(
                        fullName: $"{Faker.Name.Last()} {Faker.Name.Middle()} {Faker.Name.First()}",
                        birthdate: startDate.AddDays(gen.Next(range)).ToString().Substring(0, 10),
                        gender: Faker.Enum.Random<Gender>().ToString()
                        );
                    counter++;
                }
                return res;
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                Environment.Exit(1);
            }
            return null;
        }
        static private async Task batchInsertExecutor(string connectionString, Employee[] data)
        {
            try
            {
                await using var dataSource = NpgsqlDataSource.Create(connectionString);
                await using var connection = await dataSource.OpenConnectionAsync();

                await using var batch = new NpgsqlBatch(connection);

                foreach (var record in data)
                {
                    string insertQuery = $@"INSERT INTO test.employee(fio, birthdate, gender)
                    VALUES ($1, $2, $3)";
                    var command = new NpgsqlBatchCommand(insertQuery);
                    command.Parameters.AddWithValue(record.fullName);
                    command.Parameters.AddWithValue(NpgsqlDbType.Date, record.birthdate_date_only);
                    command.Parameters.AddWithValue(record.gender);

                    batch.BatchCommands.Add(command);
                }

                await batch.ExecuteNonQueryAsync();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                Environment.Exit(1);
            }
        }


        static public async Task insertHundredRowsMaleFamiliaStartWithF(string connectionString)
        {
            Employee[] hundredRecords = Employee.createRecords(100);
            foreach(var record in hundredRecords)
            {
                record.fullName = 'F' + record.fullName.Substring(1);
                record.gender = "Male";
            }
            await Employee.batchInsertExecutor(connectionString, hundredRecords);
        } 

        
        //пункт 5
        static public async Task selectWithConditionMaleAndFamiliaStartWithF(string connectionString)
        {
            var sw = new Stopwatch(); //позволяет измерять затраченное на работу участка кода время
            sw.Start();
            string selectQuery = $@"SELECT fio, birthdate, gender FROM test.employee WHERE fio LIKE 'F%' AND gender = 'Male'";
            await Employee.selectExecutor(connectionString, selectQuery);
            sw.Stop();
            Console.WriteLine($"Затраченное время на операцию: {sw.Elapsed}");
        }

        static private async Task selectExecutor(string connectionString, string selectQuery)
        {
            try
            {
                await using var dataSource = NpgsqlDataSource.Create(connectionString);
                await using var connection = await dataSource.OpenConnectionAsync();

                await using var command = new NpgsqlCommand(selectQuery, connection);
                await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
                string datebirth;
                while (await reader.ReadAsync())
                {
                    datebirth = reader["birthdate"].ToString().Substring(0, 10); //из Postgres приходит время в формата "дата время",хотя у столбца тип date,
                                                                                 //поэтому в данной строчке убирается "время"
                    Console.WriteLine($@"{reader["fio"]} {datebirth} {reader["gender"]}");
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                Environment.Exit(1);
            }
        }

        

        //для отладки
        public override string ToString()
        {
            return this._fullName + ' ' + this._birthdate + ' ' + this._gender;
        }
    }
}
