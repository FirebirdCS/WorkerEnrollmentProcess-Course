﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace ServiceReference
{
    using System.Runtime.Serialization;
    
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.Runtime.Serialization.DataContractAttribute(Name="EnrollmentRequest", Namespace="http://tempuri.org/")]
    public partial class EnrollmentRequest : object
    {
        
        private string NoExpedienteField;
        
        private string CicloField;
        
        private int MesInicioPagoField;
        
        private string CarreraIdField;
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false)]
        public string NoExpediente
        {
            get
            {
                return this.NoExpedienteField;
            }
            set
            {
                this.NoExpedienteField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=1)]
        public string Ciclo
        {
            get
            {
                return this.CicloField;
            }
            set
            {
                this.CicloField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(IsRequired=true, Order=2)]
        public int MesInicioPago
        {
            get
            {
                return this.MesInicioPagoField;
            }
            set
            {
                this.MesInicioPagoField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=3)]
        public string CarreraId
        {
            get
            {
                return this.CarreraIdField;
            }
            set
            {
                this.CarreraIdField = value;
            }
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.Runtime.Serialization.DataContractAttribute(Name="EnrollmentResponse", Namespace="http://tempuri.org/")]
    public partial class EnrollmentResponse : object
    {
        
        private int CodigoField;
        
        private string RespuestaField;
        
        private string CarneField;
        
        [System.Runtime.Serialization.DataMemberAttribute(IsRequired=true)]
        public int Codigo
        {
            get
            {
                return this.CodigoField;
            }
            set
            {
                this.CodigoField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false)]
        public string Respuesta
        {
            get
            {
                return this.RespuestaField;
            }
            set
            {
                this.RespuestaField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=2)]
        public string Carne
        {
            get
            {
                return this.CarneField;
            }
            set
            {
                this.CarneField = value;
            }
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.Runtime.Serialization.DataContractAttribute(Name="CandidateRecordRequest", Namespace="http://tempuri.org/")]
    public partial class CandidateRecordRequest : object
    {
        
        private string ApellidosField;
        
        private string NombresField;
        
        private string DireccionField;
        
        private string TelefonoField;
        
        private string EmailField;
        
        private string CarreraIdField;
        
        private string ExamenIdField;
        
        private string JornadaIdField;
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false)]
        public string Apellidos
        {
            get
            {
                return this.ApellidosField;
            }
            set
            {
                this.ApellidosField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false)]
        public string Nombres
        {
            get
            {
                return this.NombresField;
            }
            set
            {
                this.NombresField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=2)]
        public string Direccion
        {
            get
            {
                return this.DireccionField;
            }
            set
            {
                this.DireccionField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=3)]
        public string Telefono
        {
            get
            {
                return this.TelefonoField;
            }
            set
            {
                this.TelefonoField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=4)]
        public string Email
        {
            get
            {
                return this.EmailField;
            }
            set
            {
                this.EmailField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=5)]
        public string CarreraId
        {
            get
            {
                return this.CarreraIdField;
            }
            set
            {
                this.CarreraIdField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=6)]
        public string ExamenId
        {
            get
            {
                return this.ExamenIdField;
            }
            set
            {
                this.ExamenIdField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=7)]
        public string JornadaId
        {
            get
            {
                return this.JornadaIdField;
            }
            set
            {
                this.JornadaIdField = value;
            }
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.Runtime.Serialization.DataContractAttribute(Name="CandidateRecordResponse", Namespace="http://tempuri.org/")]
    public partial class CandidateRecordResponse : object
    {
        
        private int CodigoField;
        
        private string RespuestaField;
        
        private string NoExpedienteField;
        
        [System.Runtime.Serialization.DataMemberAttribute(IsRequired=true)]
        public int Codigo
        {
            get
            {
                return this.CodigoField;
            }
            set
            {
                this.CodigoField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false)]
        public string Respuesta
        {
            get
            {
                return this.RespuestaField;
            }
            set
            {
                this.RespuestaField = value;
            }
        }
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=2)]
        public string NoExpediente
        {
            get
            {
                return this.NoExpedienteField;
            }
            set
            {
                this.NoExpedienteField = value;
            }
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ServiceModel.ServiceContractAttribute(ConfigurationName="ServiceReference.IEnrollmentService")]
    public interface IEnrollmentService
    {
        
        [System.ServiceModel.OperationContractAttribute(Action="http://tempuri.org/IEnrollmentService/Test", ReplyAction="*")]
        System.Threading.Tasks.Task<ServiceReference.TestResponse> TestAsync(ServiceReference.TestRequest request);
        
        [System.ServiceModel.OperationContractAttribute(Action="http://tempuri.org/IEnrollmentService/EnrollmentProcess", ReplyAction="*")]
        System.Threading.Tasks.Task<ServiceReference.EnrollmentProcessResponse> EnrollmentProcessAsync(ServiceReference.EnrollmentProcessRequest request);
        
        [System.ServiceModel.OperationContractAttribute(Action="http://tempuri.org/IEnrollmentService/CandidateRegisterProcess", ReplyAction="*")]
        System.Threading.Tasks.Task<ServiceReference.CandidateRegisterProcessResponse> CandidateRegisterProcessAsync(ServiceReference.CandidateRegisterProcessRequest request);
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.ServiceModel.MessageContractAttribute(IsWrapped=false)]
    public partial class TestRequest
    {
        
        [System.ServiceModel.MessageBodyMemberAttribute(Name="Test", Namespace="http://tempuri.org/", Order=0)]
        public ServiceReference.TestRequestBody Body;
        
        public TestRequest()
        {
        }
        
        public TestRequest(ServiceReference.TestRequestBody Body)
        {
            this.Body = Body;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.Runtime.Serialization.DataContractAttribute(Namespace="http://tempuri.org/")]
    public partial class TestRequestBody
    {
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=0)]
        public string s;
        
        public TestRequestBody()
        {
        }
        
        public TestRequestBody(string s)
        {
            this.s = s;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.ServiceModel.MessageContractAttribute(IsWrapped=false)]
    public partial class TestResponse
    {
        
        [System.ServiceModel.MessageBodyMemberAttribute(Name="TestResponse", Namespace="http://tempuri.org/", Order=0)]
        public ServiceReference.TestResponseBody Body;
        
        public TestResponse()
        {
        }
        
        public TestResponse(ServiceReference.TestResponseBody Body)
        {
            this.Body = Body;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.Runtime.Serialization.DataContractAttribute(Namespace="http://tempuri.org/")]
    public partial class TestResponseBody
    {
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=0)]
        public string TestResult;
        
        public TestResponseBody()
        {
        }
        
        public TestResponseBody(string TestResult)
        {
            this.TestResult = TestResult;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.ServiceModel.MessageContractAttribute(IsWrapped=false)]
    public partial class EnrollmentProcessRequest
    {
        
        [System.ServiceModel.MessageBodyMemberAttribute(Name="EnrollmentProcess", Namespace="http://tempuri.org/", Order=0)]
        public ServiceReference.EnrollmentProcessRequestBody Body;
        
        public EnrollmentProcessRequest()
        {
        }
        
        public EnrollmentProcessRequest(ServiceReference.EnrollmentProcessRequestBody Body)
        {
            this.Body = Body;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.Runtime.Serialization.DataContractAttribute(Namespace="http://tempuri.org/")]
    public partial class EnrollmentProcessRequestBody
    {
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=0)]
        public ServiceReference.EnrollmentRequest request;
        
        public EnrollmentProcessRequestBody()
        {
        }
        
        public EnrollmentProcessRequestBody(ServiceReference.EnrollmentRequest request)
        {
            this.request = request;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.ServiceModel.MessageContractAttribute(IsWrapped=false)]
    public partial class EnrollmentProcessResponse
    {
        
        [System.ServiceModel.MessageBodyMemberAttribute(Name="EnrollmentProcessResponse", Namespace="http://tempuri.org/", Order=0)]
        public ServiceReference.EnrollmentProcessResponseBody Body;
        
        public EnrollmentProcessResponse()
        {
        }
        
        public EnrollmentProcessResponse(ServiceReference.EnrollmentProcessResponseBody Body)
        {
            this.Body = Body;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.Runtime.Serialization.DataContractAttribute(Namespace="http://tempuri.org/")]
    public partial class EnrollmentProcessResponseBody
    {
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=0)]
        public ServiceReference.EnrollmentResponse EnrollmentProcessResult;
        
        public EnrollmentProcessResponseBody()
        {
        }
        
        public EnrollmentProcessResponseBody(ServiceReference.EnrollmentResponse EnrollmentProcessResult)
        {
            this.EnrollmentProcessResult = EnrollmentProcessResult;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.ServiceModel.MessageContractAttribute(IsWrapped=false)]
    public partial class CandidateRegisterProcessRequest
    {
        
        [System.ServiceModel.MessageBodyMemberAttribute(Name="CandidateRegisterProcess", Namespace="http://tempuri.org/", Order=0)]
        public ServiceReference.CandidateRegisterProcessRequestBody Body;
        
        public CandidateRegisterProcessRequest()
        {
        }
        
        public CandidateRegisterProcessRequest(ServiceReference.CandidateRegisterProcessRequestBody Body)
        {
            this.Body = Body;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.Runtime.Serialization.DataContractAttribute(Namespace="http://tempuri.org/")]
    public partial class CandidateRegisterProcessRequestBody
    {
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=0)]
        public ServiceReference.CandidateRecordRequest request;
        
        public CandidateRegisterProcessRequestBody()
        {
        }
        
        public CandidateRegisterProcessRequestBody(ServiceReference.CandidateRecordRequest request)
        {
            this.request = request;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.ServiceModel.MessageContractAttribute(IsWrapped=false)]
    public partial class CandidateRegisterProcessResponse
    {
        
        [System.ServiceModel.MessageBodyMemberAttribute(Name="CandidateRegisterProcessResponse", Namespace="http://tempuri.org/", Order=0)]
        public ServiceReference.CandidateRegisterProcessResponseBody Body;
        
        public CandidateRegisterProcessResponse()
        {
        }
        
        public CandidateRegisterProcessResponse(ServiceReference.CandidateRegisterProcessResponseBody Body)
        {
            this.Body = Body;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
    [System.Runtime.Serialization.DataContractAttribute(Namespace="http://tempuri.org/")]
    public partial class CandidateRegisterProcessResponseBody
    {
        
        [System.Runtime.Serialization.DataMemberAttribute(EmitDefaultValue=false, Order=0)]
        public ServiceReference.CandidateRecordResponse CandidateRegisterProcessResult;
        
        public CandidateRegisterProcessResponseBody()
        {
        }
        
        public CandidateRegisterProcessResponseBody(ServiceReference.CandidateRecordResponse CandidateRegisterProcessResult)
        {
            this.CandidateRegisterProcessResult = CandidateRegisterProcessResult;
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    public interface IEnrollmentServiceChannel : ServiceReference.IEnrollmentService, System.ServiceModel.IClientChannel
    {
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.Tools.ServiceModel.Svcutil", "2.0.3")]
    public partial class EnrollmentServiceClient : System.ServiceModel.ClientBase<ServiceReference.IEnrollmentService>, ServiceReference.IEnrollmentService
    {
        
        /// <summary>
        /// Implement this partial method to configure the service endpoint.
        /// </summary>
        /// <param name="serviceEndpoint">The endpoint to configure</param>
        /// <param name="clientCredentials">The client credentials</param>
        static partial void ConfigureEndpoint(System.ServiceModel.Description.ServiceEndpoint serviceEndpoint, System.ServiceModel.Description.ClientCredentials clientCredentials);
        
        public EnrollmentServiceClient() : 
                base(EnrollmentServiceClient.GetDefaultBinding(), EnrollmentServiceClient.GetDefaultEndpointAddress())
        {
            this.Endpoint.Name = EndpointConfiguration.BasicHttpBinding_IEnrollmentService_soap.ToString();
            ConfigureEndpoint(this.Endpoint, this.ClientCredentials);
        }
        
        public EnrollmentServiceClient(EndpointConfiguration endpointConfiguration) : 
                base(EnrollmentServiceClient.GetBindingForEndpoint(endpointConfiguration), EnrollmentServiceClient.GetEndpointAddress(endpointConfiguration))
        {
            this.Endpoint.Name = endpointConfiguration.ToString();
            ConfigureEndpoint(this.Endpoint, this.ClientCredentials);
        }
        
        public EnrollmentServiceClient(EndpointConfiguration endpointConfiguration, string remoteAddress) : 
                base(EnrollmentServiceClient.GetBindingForEndpoint(endpointConfiguration), new System.ServiceModel.EndpointAddress(remoteAddress))
        {
            this.Endpoint.Name = endpointConfiguration.ToString();
            ConfigureEndpoint(this.Endpoint, this.ClientCredentials);
        }
        
        public EnrollmentServiceClient(EndpointConfiguration endpointConfiguration, System.ServiceModel.EndpointAddress remoteAddress) : 
                base(EnrollmentServiceClient.GetBindingForEndpoint(endpointConfiguration), remoteAddress)
        {
            this.Endpoint.Name = endpointConfiguration.ToString();
            ConfigureEndpoint(this.Endpoint, this.ClientCredentials);
        }
        
        public EnrollmentServiceClient(System.ServiceModel.Channels.Binding binding, System.ServiceModel.EndpointAddress remoteAddress) : 
                base(binding, remoteAddress)
        {
        }
        
        [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
        System.Threading.Tasks.Task<ServiceReference.TestResponse> ServiceReference.IEnrollmentService.TestAsync(ServiceReference.TestRequest request)
        {
            return base.Channel.TestAsync(request);
        }
        
        public System.Threading.Tasks.Task<ServiceReference.TestResponse> TestAsync(string s)
        {
            ServiceReference.TestRequest inValue = new ServiceReference.TestRequest();
            inValue.Body = new ServiceReference.TestRequestBody();
            inValue.Body.s = s;
            return ((ServiceReference.IEnrollmentService)(this)).TestAsync(inValue);
        }
        
        [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
        System.Threading.Tasks.Task<ServiceReference.EnrollmentProcessResponse> ServiceReference.IEnrollmentService.EnrollmentProcessAsync(ServiceReference.EnrollmentProcessRequest request)
        {
            return base.Channel.EnrollmentProcessAsync(request);
        }
        
        public System.Threading.Tasks.Task<ServiceReference.EnrollmentProcessResponse> EnrollmentProcessAsync(ServiceReference.EnrollmentRequest request)
        {
            ServiceReference.EnrollmentProcessRequest inValue = new ServiceReference.EnrollmentProcessRequest();
            inValue.Body = new ServiceReference.EnrollmentProcessRequestBody();
            inValue.Body.request = request;
            return ((ServiceReference.IEnrollmentService)(this)).EnrollmentProcessAsync(inValue);
        }
        
        [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
        System.Threading.Tasks.Task<ServiceReference.CandidateRegisterProcessResponse> ServiceReference.IEnrollmentService.CandidateRegisterProcessAsync(ServiceReference.CandidateRegisterProcessRequest request)
        {
            return base.Channel.CandidateRegisterProcessAsync(request);
        }
        
        public System.Threading.Tasks.Task<ServiceReference.CandidateRegisterProcessResponse> CandidateRegisterProcessAsync(ServiceReference.CandidateRecordRequest request)
        {
            ServiceReference.CandidateRegisterProcessRequest inValue = new ServiceReference.CandidateRegisterProcessRequest();
            inValue.Body = new ServiceReference.CandidateRegisterProcessRequestBody();
            inValue.Body.request = request;
            return ((ServiceReference.IEnrollmentService)(this)).CandidateRegisterProcessAsync(inValue);
        }
        
        public virtual System.Threading.Tasks.Task OpenAsync()
        {
            return System.Threading.Tasks.Task.Factory.FromAsync(((System.ServiceModel.ICommunicationObject)(this)).BeginOpen(null, null), new System.Action<System.IAsyncResult>(((System.ServiceModel.ICommunicationObject)(this)).EndOpen));
        }
        
        public virtual System.Threading.Tasks.Task CloseAsync()
        {
            return System.Threading.Tasks.Task.Factory.FromAsync(((System.ServiceModel.ICommunicationObject)(this)).BeginClose(null, null), new System.Action<System.IAsyncResult>(((System.ServiceModel.ICommunicationObject)(this)).EndClose));
        }
        
        private static System.ServiceModel.Channels.Binding GetBindingForEndpoint(EndpointConfiguration endpointConfiguration)
        {
            if ((endpointConfiguration == EndpointConfiguration.BasicHttpBinding_IEnrollmentService_soap))
            {
                System.ServiceModel.BasicHttpBinding result = new System.ServiceModel.BasicHttpBinding();
                result.MaxBufferSize = int.MaxValue;
                result.ReaderQuotas = System.Xml.XmlDictionaryReaderQuotas.Max;
                result.MaxReceivedMessageSize = int.MaxValue;
                result.AllowCookies = true;
                return result;
            }
            throw new System.InvalidOperationException(string.Format("Could not find endpoint with name \'{0}\'.", endpointConfiguration));
        }
        
        private static System.ServiceModel.EndpointAddress GetEndpointAddress(EndpointConfiguration endpointConfiguration)
        {
            if ((endpointConfiguration == EndpointConfiguration.BasicHttpBinding_IEnrollmentService_soap))
            {
                return new System.ServiceModel.EndpointAddress("http://localhost:5267/EnrollmentService.asmx");
            }
            throw new System.InvalidOperationException(string.Format("Could not find endpoint with name \'{0}\'.", endpointConfiguration));
        }
        
        private static System.ServiceModel.Channels.Binding GetDefaultBinding()
        {
            return EnrollmentServiceClient.GetBindingForEndpoint(EndpointConfiguration.BasicHttpBinding_IEnrollmentService_soap);
        }
        
        private static System.ServiceModel.EndpointAddress GetDefaultEndpointAddress()
        {
            return EnrollmentServiceClient.GetEndpointAddress(EndpointConfiguration.BasicHttpBinding_IEnrollmentService_soap);
        }
        
        public enum EndpointConfiguration
        {
            
            BasicHttpBinding_IEnrollmentService_soap,
        }
    }
}
