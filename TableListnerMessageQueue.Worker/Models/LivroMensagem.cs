using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TableListnerMessageQueue.Worker.Models
{
    public class LivroMensagem
    {
        public long AutorId { get; set; }
        public string Titulo { get; set; }
        public int NumPaginas { get; set; }
        public string Categoria { get; set; }
    }
}
