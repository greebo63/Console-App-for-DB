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
                    //if (DateOnly.TryParseExact(value, "yyyy-mm-dd", out value_to_date))
                    if (DateOnly.TryParse(value, out value_to_date))
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
                }
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
                }
            }
        }

        //конструкторы
        public Employee(string fullName, string birthdate, string gender)
        {
            try
            {
                this.fullName = fullName;
                this.birthdate = birthdate;
                this.gender = gender;
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Произошла ошибка: {ex.Message}");
            }
        }

        //методы
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
            }
            
        }

        public int ageCounter()
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

        static public int ageCounter(string date)
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
            try
            {
                string insertIntoTableQuery = @"
                    INSERT INTO test.employee(fio, birthdate, gender) 
                    VALUES ($1, $2, $3)
                    ";   //позиционные placeholders выбраны в связи с увеличением производительности,
                         //т.к. под капотом npgsql все равно именованные placeholders переводит в позиционные placeholders

                await using var dataSource = NpgsqlDataSource.Create(connectionString);
                await using var connection = await dataSource.OpenConnectionAsync();


                await using var command = new NpgsqlCommand(insertIntoTableQuery, connection);

                command.Parameters.AddWithValue(_fullName);
                command.Parameters.AddWithValue(NpgsqlDbType.Date, _birthdate);
                command.Parameters.AddWithValue(_gender);

                await command.ExecuteNonQueryAsync();

                Console.WriteLine("Значение успешно вставлено");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Произошла ошибка: {ex.Message}");
            }
        }

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

            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            await using var connection = await dataSource.OpenConnectionAsync();

            await using var command = new NpgsqlCommand(selectAllWithUnicFioDataQuery, connection);
            await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
            string datebirth;
            while (await reader.ReadAsync())
            {
                datebirth = reader["birthdate"].ToString().Substring(0, 10);
                Console.WriteLine($@"{reader["fio"]} {datebirth} {reader["gender"]} {Employee.ageCounter(datebirth)}");
            }
        }

        static public async Task insertMillionRecords(string connectionString)
        {
            Employee[] millionRecords = Employee.createMillionRecords();
            //NpgsqlBatchCommand[] batchCommands = new NpgsqlBatchCommand[millionRecords.Length];

            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            await using var connection = await dataSource.OpenConnectionAsync();

            await using var batch = new NpgsqlBatch(connection);

            foreach (var record in millionRecords)
            {
                string insertQuery = $@"INSERT INTO test.employee(fio, birthdate, gender)
                    VALUES ($1, $2, $3)";
                var command = new NpgsqlBatchCommand(insertQuery);
                command.Parameters.AddWithValue(record.fullName);
                command.Parameters.AddWithValue(NpgsqlDbType.Date, record.birthdate);
                command.Parameters.AddWithValue(record.gender);

                batch.BatchCommands.Add(command);
            }

            await batch.ExecuteNonQueryAsync();
        }

        static private Employee[] createMillionRecords()
        {
            Employee[] res = new Employee[10];

            DateTime startDate = DateTime.MinValue.AddYears(1900);
            Random gen = new Random();
            int range = ((DateTime.Today - startDate).Days);

            for (int i = 0; i < 10; i++)
            {
                res[i] = new Employee(
                    fullName: $"{Faker.Name.Last()} {Faker.Name.Middle()} {Faker.Name.First()}",
                    birthdate: startDate.AddDays(gen.Next(range)).ToString().Substring(0, 10),
                    gender: Faker.Enum.Random<Gender>().ToString()
                    );
            }
            return res;
        }

        public override string ToString()
        {
            return this._fullName + ' ' + this._birthdate + ' ' + this._gender;
        }
    }
}
