using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
using ServiceReference;
using WorkerEnrollmentProcess.Models;

public class Program
{
    private static AppLog AppLog = new AppLog();
    private static IModel channel = null;
    public static void Main(string[] args)
    {
        Log.Logger = new LoggerConfiguration()
                    .WriteTo
                    .File("WorkerEnrollmentManagement.out", Serilog.Events.LogEventLevel.Debug, "{Message:lj}{NewLine}", encoding: Encoding.UTF8)
                    .CreateLogger();
        AppLog.ResponseTime = Convert.ToInt16(DateTime.Now.ToString("fff"));
        AppLog.DateTime = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");
        AppLog.ResponseCode = 0;
        IConnection conexion = null;
        var connectionFactory = new ConnectionFactory();
        connectionFactory.Port = 5672;
        connectionFactory.UserName = "guest";
        connectionFactory.Password = "guest";
        conexion = connectionFactory.CreateConnection();
        channel = conexion.CreateModel();
        var consumerEnrollment = new EventingBasicConsumer(channel);
        var consumerCandidateRecord = new EventingBasicConsumer(channel);
        consumerEnrollment.Received += ConsumerEnrollmentReceived;
        consumerCandidateRecord.Received += ConsumerReceivedCandidateRecordReceived;
        var consumerEnrollmentTag = channel.BasicConsume("kalum.queue.enrollment", true, consumerEnrollment);
        var consumerTagCandidateRecordCreate = channel.BasicConsume("kalum.queue.candidate-record", true, consumerCandidateRecord);
        Console.WriteLine("Presione enter para finalizar el proceso");
        Console.ReadLine();
    }

    public static async void ConsumerReceivedCandidateRecordReceived(object? sender, BasicDeliverEventArgs e)
    {
        string message = Encoding.UTF8.GetString(e.Body.ToArray());
        CandidateRecordRequest request = JsonSerializer.Deserialize<CandidateRecordRequest>(message);
        ImprimirLog(0, JsonSerializer.Serialize(request), "Debug");
        CandidateRecordResponse candidateRecordResponse = await ClientWebServiceCandidateRecord(request);
        if (candidateRecordResponse != null && candidateRecordResponse.Codigo != 201)
        {
            channel.BasicPublish("", "kalum.queue.failed-enrollment", null, Encoding.UTF8.GetBytes(message));
        }
        else
        {
            AppLog.Message = $"Se creo exitosamente el registro del expediente con número {candidateRecordResponse.NoExpediente}";
            ImprimirLog(201, JsonSerializer.Serialize(AppLog), "Information");
        }
    }

    public static async void ConsumerEnrollmentReceived(object? sender, BasicDeliverEventArgs e)
    {
        string message = Encoding.UTF8.GetString(e.Body.ToArray());
        EnrollmentRequest request = JsonSerializer.Deserialize<EnrollmentRequest>(message);
        ImprimirLog(0, JsonSerializer.Serialize(request), "Debug");
        EnrollmentResponse enrollmentResponse = await ClientWebServiceEnrollment(request);
        if (enrollmentResponse != null && enrollmentResponse.Codigo != 201)
        {
            channel.BasicPublish("", "kalum.queue.failed-enrollment", null, Encoding.UTF8.GetBytes(message));
        }
        else
        {
            AppLog.Message = "Se proceso el proceso de inscripción de forma exitosa";
            ImprimirLog(201, JsonSerializer.Serialize(AppLog), "Information");
        }
    }


    public static void ImprimirLog(int responseCode, string message, string typeLog)
    {
        AppLog.ResponseCode = responseCode;
        AppLog.Message = message;
        AppLog.ResponseTime = Convert.ToInt16(DateTime.Now.ToString("fff")) - AppLog.ResponseTime;
        if (typeLog.Equals("Error"))
        {
            AppLog.Level = 40;
            Log.Error(JsonSerializer.Serialize(AppLog));
        }
        else if (typeLog.Equals("Information"))
        {
            AppLog.Level = 20;
            Log.Information(JsonSerializer.Serialize(AppLog));
        }
        else if (typeLog.Equals("Debug"))
        {
            AppLog.Level = 10;
            Log.Debug(JsonSerializer.Serialize(AppLog));
        }
    }

    public static async Task<CandidateRecordResponse> ClientWebServiceCandidateRecord(CandidateRecordRequest request)
    {
        CandidateRecordResponse candidateRecordResponse = null;
        try
        {
            var client = new EnrollmentServiceClient(EnrollmentServiceClient.EndpointConfiguration.BasicHttpBinding_IEnrollmentService_soap, "http://localhost:5181/EnrollmentService.asmx");
            var response = await client.CandidateRegisterProcessAsync(request);
            candidateRecordResponse = new CandidateRecordResponse()
            {
                Codigo = response.Body.CandidateRegisterProcessResult.Codigo,
                Respuesta = response.Body.CandidateRegisterProcessResult.Respuesta,
                NoExpediente = response.Body.CandidateRegisterProcessResult.NoExpediente
            };
            if (candidateRecordResponse.Codigo == 503)
            {
                ImprimirLog(503, candidateRecordResponse.Respuesta, "Error");
            }
            else
            {
                ImprimirLog(201, $"Se finalizo el proceso de inscripción con el Expediente {candidateRecordResponse.NoExpediente}", "Information");
            }
        }
        catch (Exception e)
        {
            candidateRecordResponse = new CandidateRecordResponse() { Codigo = 500, Respuesta = e.Message, NoExpediente = "0" };
            ImprimirLog(500, $"Error en el sistema: {e.Message}", "Error");

        }
        return candidateRecordResponse;
    }

    public static async Task<EnrollmentResponse> ClientWebServiceEnrollment(EnrollmentRequest request)
    {
        EnrollmentResponse enrollmentResponse = null;
        try
        {
            var client = new EnrollmentServiceClient(EnrollmentServiceClient.EndpointConfiguration.BasicHttpBinding_IEnrollmentService_soap, "http://localhost:5181/EnrollmentService.asmx");
            var response = await client.EnrollmentProcessAsync(request);
            enrollmentResponse = new EnrollmentResponse()
            {
                Codigo = response.Body.EnrollmentProcessResult.Codigo,
                Respuesta = response.Body.EnrollmentProcessResult.Respuesta,
                Carne = response.Body.EnrollmentProcessResult.Carne
            };
            if (enrollmentResponse.Codigo == 503)
            {
                ImprimirLog(503, enrollmentResponse.Respuesta, "Error");
            }
            else
            {
                ImprimirLog(201, $"Se finalizo el proceso de inscripción con el carné {enrollmentResponse.Carne}", "Information");
            }
        }
        catch (Exception e)
        {
            enrollmentResponse = new EnrollmentResponse() { Codigo = 500, Respuesta = e.Message, Carne = "0" };
            ImprimirLog(500, $"Error en el sistema: {e.Message}", "Error");
        }
        return enrollmentResponse;

    }
}