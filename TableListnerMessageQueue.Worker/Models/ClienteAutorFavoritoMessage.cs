using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace TableListnerMessageQueue.Worker.Models
{
    public class ClienteAutorFavoritoMessage : IMessageMQ
    {
        [JsonPropertyName("clienteId")]
        public long ClienteId { get; set; }
        [JsonPropertyName("autorFavoritoId")]
        public long AutorFavoritoId { get; set; }
    }
}
