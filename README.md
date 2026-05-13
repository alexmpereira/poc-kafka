# POC Kafka - Processamento Assíncrono de Transações Financeiras

Esta prova de conceito (POC) demonstra a integração do **Apache Kafka** em uma arquitetura de microsserviços. 
Utilizamos uma API **Node.js (TypeScript)** como *Producer* (produtor de eventos) e um *Worker* em **PHP** como *Consumer* (consumidor de eventos), rodando de maneira simplificada com Docker Compose (usando Kafka KRaft, sem necessidade de Zookeeper).

## 🏢 O Problema Real

**Cenário:** Um sistema de pagamentos de transações financeiras (ex: e-commerce ou carteira digital PIX).

**O problema:** Quando um usuário realiza um pagamento, a validação de fraude, comunicação com adquirente, atualização de saldo no ERP e envio de e-mail podem demorar alguns segundos. Se a API Node.js de pagamento esperar tudo isso acontecer para responder, o usuário enfrentará lentidão (timeout), a experiência será ruim, e o sistema terá um alto acoplamento (se o serviço de envio de e-mail falhar, a requisição inteira de pagamento falha).

**A solução com Kafka:** 
1. A API (Node.js) recebe a requisição de transação.
2. Ao invés de processar tudo sincronicamente, ela apenas "avisa" que uma transação ocorreu publicando um evento no tópico `financial-transactions` no Kafka.
3. A API responde rapidamente para o usuário: "Transação recebida e em processamento" (Status HTTP 202).
4. O Worker em PHP assina (*subscribe*) esse tópico no Kafka, retira a mensagem do final da fila assim que ela chega, e processa as regras de negócio de forma assíncrona.

---

## 🚀 Como executar o projeto localmente via Docker

### Pré-requisitos
- Docker
- Docker Compose

### Passos

1. Clone ou acesse o diretório do projeto.
2. Na raiz do projeto, construa as imagens e suba os containers em background com o comando:
   ```bash
   docker compose up -d --build
   ```
3. Aguarde alguns instantes até que o Kafka seja totalmente inicializado (você pode acompanhar com `docker compose logs -f kafka`).
4. Verifique se os três serviços estão rodando:
   ```bash
   docker compose ps
   ```
   Você deverá ver os containers: `poc-kafka`, `poc-node-producer` e `poc-php-consumer`.

---

## 🧪 Como simular o teste

1. **Abra os logs do Consumer PHP** para vermos o processamento assíncrono acontecendo em tempo real:
   ```bash
   docker compose logs -f php-consumer
   ```

2. **Dispare uma requisição para a API Node.js**. Em outro terminal, use o `curl` (ou um software como Postman/Insomnia) para simular uma transação financeira na porta `3000`:
   ```bash
   curl -X POST http://localhost:3000/transactions \
     -H "Content-Type: application/json" \
     -d '{
       "userId": "usr_98765",
       "amount": 250.50,
       "type": "PIX"
     }'
   ```

3. **Valide os resultados:**
   - No terminal do `curl`, você receberá imediatamente a resposta rápida da API:
     ```json
     {
       "message": "Transaction received and is being processed.",
       "transaction": {
         "id": "a1b2c3d",
         "userId": "usr_98765",
         "amount": 250.5,
         "type": "PIX",
         "timestamp": "2023-10-25T12:00:00Z"
       }
     }
     ```
   - No terminal dos **logs do PHP**, você verá o *Consumer* capturando o evento publicado no Kafka e simulando o processamento assíncrono (com uma pausa de 2 segundos simulando a validação):
     ```text
     ======================================
     [2023-10-25 12:00:00] Event Received!
     -> Transaction ID: a1b2c3d
     -> User ID: usr_98765
     -> Amount: $ 250.50
     -> Type: PIX
     ⏳ Processing transaction (fraud analysis, balance check)...
     ✅ Transaction successfully processed!
     ======================================
     ```

---

## 🧠 Conceitos do Kafka

O Apache Kafka é uma plataforma distribuída de *streaming* de eventos. Ao contrário do RabbitMQ (que foca na entrega tradicional de filas de mensagens em memória e depois apaga a mensagem), o Kafka atua como um **log de eventos persistente e distribuído** (*append-only log*). 

### Quando e Como deve ser usado?

O Kafka brilha em cenários onde você tem um alto volume de dados (High Throughput) e onde múltiplos sistemas diferentes precisam reagir ao mesmo evento.
- **Deve ser usado para:** Desacoplamento de microsserviços, arquiteturas orientadas a eventos (Event-Driven Architecture), *stream processing* (análise de dados e métricas em tempo real), log aggregation, e sistemas de alta volumetria.
- **Não deve ser usado para:** Tarefas que requerem respostas síncronas imediatas (ex: API precisa do ID retornado do banco de dados pelo worker antes de responder), roteamento muito complexo baseado no header da mensagem (nesse caso, RabbitMQ com *Topic/Direct Exchanges* pode ser mais adequado), ou pequenos projetos triviais.

### 🟢 Pontos Positivos
- **Desacoplamento Extremo:** Produtores e consumidores não dependem um do outro e nem de disponibilidade em tempo real. Se o container PHP cair ou for reiniciado, o Node.js continua aceitando requisições e enfileirando dados no Kafka. Quando o PHP voltar, ele lê do ponto exato de onde parou (*offset*).
- **Escalabilidade:** Projetado para lidar com milhões de mensagens por segundo distribuídas em partições num cluster horizontal.
- **Replay de Eventos:** As mensagens ficam armazenadas em disco por um tempo configurável (por padrão 7 dias). Você pode adicionar um microsserviço novo no mês que vem, e mandar ele ler eventos ("Replay") que ocorreram na semana anterior.
- **Múltiplos Consumidores Independenes:** Um evento "Transação Recebida" pode ser consumido simultaneamente por um Worker Anti-Fraude (Python), um Worker de Emissão de Nota (PHP) e um Data Lake (Java) através de diferentes *Consumer Groups*, sem um interferir na fila do outro.

### 🔴 Pontos Negativos
- **Complexidade Operacional:** Administrar clusters de Kafka com dezenas de *brokers*, configurar segurança (SSL/SASL), partições e *retention policies* exige um conhecimento de infraestrutura superior a gerenciadores de filas tradicionais.
- **Curva de Aprendizado:** Desenvolvedores precisam dominar os conceitos de Partições, Consumer Groups, Offsets, Brokers, Replication Factor e estratégias de *commit* (garantia de entrega At-Least-Once ou Exactly-Once) para extrair o real valor.
- **Overkill em Projetos Simples:** Adicionar o Apache Kafka à stack apenas para disparar o envio de um único e-mail esporádico é considerado *"overengineering"*. Para fluxos pequenos, Redis Pub/Sub, Amazon SQS, ou filas simples de banco de dados muitas vezes resolvem perfeitamente o problema com uma fração da complexidade.