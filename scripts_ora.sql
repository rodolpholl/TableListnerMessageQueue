-- ###############################################################
-- # SCRIPT DE CONFIGURAÇÃO ORACLE PARA INTEGRAÇÃO COM RABBITMQ #
-- ###############################################################

-- Este script contém a criação de sequences, tabelas, configuração do
-- Oracle Advanced Queuing (AQ), um procedimento PL/SQL e um trigger
-- para automatizar o envio de mensagens para o AQ quando um novo
-- autor é inserido.

-- Conecte-se com um usuário que tenha privilégios para criar objetos
-- de banco de dados (ex: DBA, SYSTEM, ou um usuário com as permissões
-- necessárias).

--------------------------------------------------------------------
-- 1. Criação das Sequences
--------------------------------------------------------------------

-- Sequence para a tabela AUTOR, para preenchimento automático do Id
CREATE SEQUENCE AUTOR_ID_SEQ
START WITH 1
INCREMENT BY 1
NOCACHE
NOCYCLE;
/

-- Sequence para a tabela LIVRO, para preenchimento automático do Id
CREATE SEQUENCE LIVRO_ID_SEQ
START WITH 1
INCREMENT BY 1
NOCACHE
NOCYCLE;
/

--------------------------------------------------------------------
-- 2. Criação das Tabelas AUTOR e LIVRO
--------------------------------------------------------------------

-- Tabela AUTOR
CREATE TABLE AUTOR (
    Id            NUMBER(19) DEFAULT AUTOR_ID_SEQ.NEXTVAL PRIMARY KEY,
    Nome          VARCHAR2(150) NOT NULL,
    DataNascimento DATE,
    Email         VARCHAR2(150) UNIQUE,
    Phone         VARCHAR2(30)
);
/

-- Tabela LIVRO
CREATE TABLE LIVRO (
    Id            NUMBER(19) DEFAULT LIVRO_ID_SEQ.NEXTVAL PRIMARY KEY,
    Autor_Id      NUMBER(19) NOT NULL,
    Titulo        VARCHAR2(300) NOT NULL,
    Num_Paginas   NUMBER(5),
    Categoria     VARCHAR2(150),
    CONSTRAINT FK_LIVRO_AUTOR FOREIGN KEY (Autor_Id) REFERENCES AUTOR(Id)
);
/

-- Opcional: Adicionar comentários para documentar as tabelas
COMMENT ON TABLE AUTOR IS 'Armazena informações sobre os autores de livros.';
COMMENT ON COLUMN AUTOR.Id IS 'Identificador único do autor.';
COMMENT ON COLUMN AUTOR.Nome IS 'Nome completo do autor.';
COMMENT ON COLUMN AUTOR.DataNascimento IS 'Data de nascimento do autor.';
COMMENT ON COLUMN AUTOR.Email IS 'Endereço de e-mail do autor (deve ser único).';
COMMENT ON COLUMN AUTOR.Phone IS 'Número de telefone do autor.';

COMMENT ON TABLE LIVRO IS 'Armazena informações sobre os livros.';
COMMENT ON COLUMN LIVRO.Id IS 'Identificador único do livro.';
COMMENT ON COLUMN LIVRO.Autor_Id IS 'Chave estrangeira para a tabela AUTOR, indicando o autor do livro.';
COMMENT ON COLUMN LIVRO.Titulo IS 'Título do livro.';
COMMENT ON COLUMN LIVRO.Num_Paginas IS 'Número de páginas do livro.';
COMMENT ON COLUMN LIVRO.Categoria IS 'Categoria ou gênero do livro.';

--------------------------------------------------------------------
-- 3. Configuração do Oracle Advanced Queuing (AQ)
--------------------------------------------------------------------

-- 3.1. Crie a tabela de filas (Queue Table)
-- CORREÇÃO: Usando 'SYS.AQ$_JMS_TEXT_MESSAGE' como payload_type
BEGIN
  DBMS_AQADM.CREATE_QUEUE_TABLE (
    queue_table          => 'AUTOR_JSON_QTABLE',
    queue_payload_type   => 'SYS.AQ$_JMS_TEXT_MESSAGE', -- <-- TIPO CORRIGIDO
    multiple_consumers   => FALSE       -- Permite apenas um consumidor por mensagem para simplificar
  );
END;
/

-- 3.2. Crie a fila (Queue)
BEGIN
  DBMS_AQADM.CREATE_QUEUE (
    queue_name           => 'AUTOR_NOVOS_JSON_QUEUE',
    queue_table          => 'AUTOR_JSON_QTABLE'
  );
END;
/

-- 3.3. Inicie a fila para que ela possa receber e enviar mensagens
BEGIN
  DBMS_AQADM.START_QUEUE (
    queue_name           => 'AUTOR_NOVOS_JSON_QUEUE'
  );
END;
/

-- 3.4. Conceda permissões necessárias ao usuário do banco de dados
-- Substitua 'SEU_USUARIO_DB' pelo usuário real que fará INSERTs e DEQUEUEs.
-- Este usuário precisará destas permissões para interagir com o AQ.
GRANT EXECUTE ON DBMS_AQADM TO SEU_USUARIO_DB;
GRANT EXECUTE ON DBMS_AQ TO SEU_USUARIO_DB;
GRANT SELECT, INSERT, UPDATE, DELETE ON AUTOR_JSON_QTABLE TO SEU_USUARIO_DB;
GRANT ALL ON AUTOR_NOVOS_JSON_QUEUE TO SEU_USUARIO_DB;
-- Conceda permissão para usar o tipo SYS.AQ$_JMS_TEXT_MESSAGE
GRANT EXECUTE ON SYS.AQ$_JMS_TEXT_MESSAGE TO SEU_USUARIO_DB;
/

--------------------------------------------------------------------
-- 4. Procedimento PL/SQL para Enfileirar Mensagens no AQ
--------------------------------------------------------------------

-- CORREÇÃO: Ajustado para usar o tipo SYS.AQ$_JMS_TEXT_MESSAGE
CREATE OR REPLACE PROCEDURE ENQUEUE_NOVO_AUTOR_JSON (
    p_autor_id      IN NUMBER,
    p_autor_nome    IN VARCHAR2,
    p_autor_email   IN VARCHAR2
) AS
    enqueue_options    DBMS_AQ.ENQUEUE_OPTIONS_T;
    message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;
    message_handle     RAW(16);
    json_string        VARCHAR2(4000); -- A string JSON que você quer enviar
    aq_message_obj     SYS.AQ$_JMS_TEXT_MESSAGE; -- Objeto do tipo de payload
BEGIN
    -- Monta a string JSON com os dados relevantes do novo autor
    -- Usando REPLACE para escapar aspas duplas, caso o nome ou email contenham
    json_string := '{"Id":' || p_autor_id || ',"Nome":"' || REPLACE(p_autor_nome, '"', '"') || '","Email":"' || REPLACE(p_autor_email, '"', '"') || '"}';

    -- Cria um novo objeto JMS_TEXT_MESSAGE
    aq_message_obj := SYS.AQ$_JMS_TEXT_MESSAGE.construct();
    aq_message_obj.set_text(json_string); -- Define o texto da mensagem

    DBMS_AQ.ENQUEUE(
        queue_name         => 'AUTOR_NOVOS_JSON_QUEUE',
        enqueue_options    => enqueue_options,
        message_properties => message_properties,
        payload            => aq_message_obj, -- <-- AGORA PASSAMOS O OBJETO JMS
        msgid              => message_handle
    );
    -- IMPORTANTE: O COMMIT aqui não é necessário e deve ser evitado.
    -- O COMMIT será realizado pela transação que inseriu o registro na tabela AUTOR.
END;
/

--------------------------------------------------------------------
-- 5. Trigger AFTER INSERT na Tabela AUTOR
--------------------------------------------------------------------

-- Este trigger será disparado automaticamente após a inserção de
-- um novo registro na tabela AUTOR.
CREATE OR REPLACE TRIGGER TRG_AUTOR_AFTER_INSERT
AFTER INSERT ON AUTOR
FOR EACH ROW
BEGIN
    -- Chama o procedimento para enfileirar os dados do novo autor
    -- na fila do Oracle AQ para processamento externo.
    ENQUEUE_NOVO_AUTOR_JSON(
        p_autor_id    => :NEW.Id,      -- Id do autor recém-inserido
        p_autor_nome  => :NEW.Nome,    -- Nome do autor recém-inserido
        p_autor_email => :NEW.Email    -- E-mail do autor recém-inserido
    );
END;
/

-- Fim do script de configuração Oracle.
-- Após a execução, verifique a criação de todos os objetos.