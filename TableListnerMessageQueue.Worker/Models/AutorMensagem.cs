using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TableListnerMessageQueue.Worker.Models
{
    public class AutorMensagem : IMessageMQ
    {
        public long Id { get; set; } = default!;
        public string Nome { get; set; } = default!;
        public string Email { get; set; } = default!;
    }
}
