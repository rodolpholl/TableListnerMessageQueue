using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TableListnerMessageQueue.Worker.Models
{
    public class LivroMensagem : IMessageMQ
    {
        public long AutorId { get; set; } = default!;
        public string Titulo { get; set; } = default!;
        public int NumPaginas { get; set; } = default!;
        public string Categoria { get; set; } = default!;
    }
}
