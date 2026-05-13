# Middleware Publish/Subscribe - Programação Distribuída

Implementação de um middleware Publish/Subscribe (Pub/Sub) em Go para troca de mensagens entre processos distribuídos.

A solução foi desenvolvida em **Go**, utilizando comunicação **TCP + JSON** entre clientes e broker.

## Sobre o projeto

Este projeto foi desenvolvido para a disciplina de **Programação Distribuída** com o objetivo de implementar um middleware baseado na arquitetura **Publish/Subscribe**, contendo:

- um **broker** responsável por gerenciar a comunicação entre os clientes;
- uma **biblioteca cliente** para abstrair o uso do middleware;
- **aplicações de exemplo** para publicação e consumo de mensagens.

No sistema implementado, as mensagens possuem dois campos principais:

- um **tópico**;
- um **conteúdo em JSON**, enviado como string.

A proposta do projeto é permitir que clientes publiquem mensagens em tópicos e que outros clientes possam se inscrever nesses tópicos para receber as mensagens correspondentes. Além disso, o broker gerencia a criação e remoção dinâmica de tópicos, o descarte de mensagens sem inscritos e a independência entre recebimento e encaminhamento de mensagens.

## Objetivo

O objetivo deste projeto é construir um middleware Pub/Sub funcional, demonstrando:

- comunicação entre processos distribuídos;
- desacoplamento entre publicadores e consumidores;
- gerenciamento dinâmico de tópicos;
- envio e recebimento de mensagens por meio de um broker;
- abstração do middleware por meio de uma biblioteca cliente;
- cenário de teste com múltiplos tópicos e múltiplos clientes.

## Funcionalidades implementadas

Atualmente, o sistema já suporta:

- conexão de clientes ao broker;
- publicação de mensagens;
- inscrição em tópicos;
- remoção de inscrição em tópicos;
- criação dinâmica de tópicos;
- remoção de tópicos sem inscritos;
- descarte de mensagens em tópicos sem inscritos;
- encaminhamento de mensagens para todos os clientes inscritos em um tópico;
- uso de filas por tópico para desacoplar recebimento e encaminhamento.

## Arquitetura da solução

A arquitetura atual é composta por três partes principais:

### 1. Broker

O broker é o componente central do middleware. Ele é responsável por:

- aceitar conexões TCP de clientes;
- registrar inscrições em tópicos;
- remover inscrições em tópicos;
- criar tópicos em tempo de execução;
- remover tópicos sem inscritos;
- receber mensagens publicadas;
- encaminhar mensagens para os clientes inscritos;
- descartar mensagens publicadas em tópicos sem inscritos, informando isso ao publicador.

### 2. Biblioteca cliente

A biblioteca cliente abstrai a comunicação direta com o broker e fornece operações de mais alto nível para as aplicações.

Ela encapsula:

- abertura de conexão TCP;
- envio de requisições ao broker;
- leitura de respostas;
- operações de `Subscribe`, `Unsubscribe` e `Publish`.

### 3. Aplicações cliente

Foram implementadas duas aplicações de exemplo:

- **publisher**: cliente responsável por publicar mensagens em um tópico;
- **subscriber**: cliente responsável por se inscrever em um ou mais tópicos e receber mensagens publicadas, além de permitir comandos interativos para inscrição e remoção de inscrição.

## Estrutura do projeto

```text
cmd/
  broker/
    main.go
  publisher/
    main.go
  subscriber/
    main.go
internal/
  broker/
    broker.go
  client/
    client.go
  protocol/
    messages.go
go.mod
README.md
```

## Tecnologias utilizadas

* Go
* TCP
* JSON
* Goroutines
* Channels
* Mutex

## Protocolo de comunicação

A comunicação entre clientes e broker é feita por **TCP**, utilizando mensagens em **JSON**.

### Requisições enviadas pelos clientes

Exemplos de mensagens enviadas pelos clientes ao broker:

```json
{"action":"subscribe","topic":"chat"}
{"action":"unsubscribe","topic":"chat"}
{"action":"publish","topic":"chat","data":"{\"user\":\"ana\",\"msg\":\"oi\"}"}
```

### Respostas enviadas pelo broker

Exemplos de respostas enviadas pelo broker:

```json
{"type":"ack","status":"subscribed","topic":"chat"}
{"type":"ack","status":"unsubscribed","topic":"chat"}
{"type":"ack","status":"published","topic":"chat"}
{"type":"ack","status":"discarded","topic":"chat","error":"no subscribers"}
{"type":"deliver","topic":"chat","data":"{\"user\":\"ana\",\"msg\":\"oi\"}"}
```

## Como executar

### Pré-requisitos

Antes de executar o projeto, é necessário ter instalado:

* Go
* terminal com suporte para executar múltiplos processos

### 1. Clonar o repositório

```bash
git clone https://github.com/MaiconFelipedev/middleware-pubsub-programacao-distribuida.git
cd middleware-pubsub-programacao-distribuida
```

### 2. Iniciar o broker

Em um terminal, execute:

```powershell
go run .\cmd\broker
```

O broker ficará escutando conexões na porta `9000`.

### 3. Iniciar os subscribers

Em um segundo terminal, execute:

```powershell
go run .\cmd\subscriber chat temperatura
```

Em um terceiro terminal, execute:

```powershell
go run .\cmd\subscriber alerta pedido
```

### 4. Publicar mensagens

Em um quarto terminal, execute:

```powershell
go run .\cmd\publisher chat "{\"user\":\"ana\",\"msg\":\"oi\"}"
go run .\cmd\publisher temperatura "{\"valor\":28}"
go run .\cmd\publisher alerta "{\"nivel\":\"alto\"}"
go run .\cmd\publisher pedido "{\"id\":123,\"status\":\"novo\"}"
```

## Comandos disponíveis no subscriber

Após iniciar, o subscriber aceita comandos interativos no terminal:

```text
sub <topico>
unsub <topico>
quit
```

### Significado dos comandos

* `sub <topico>`: inscreve o cliente em um novo tópico;
* `unsub <topico>`: remove a inscrição do cliente em um tópico;
* `quit`: encerra o subscriber.

## Cenário de teste realizado

Foi realizado um cenário de teste com:

* **1 broker**
* **2 subscribers**
* **publishers executados separadamente**
* **4 tópicos no total**:

  * `chat`
  * `temperatura`
  * `alerta`
  * `pedido`

### Organização dos subscribers

#### Subscriber 1

Inscrito nos tópicos:

* `chat`
* `temperatura`

#### Subscriber 2

Inscrito nos tópicos:

* `alerta`
* `pedido`

## Testes realizados

### Teste 1 - Publicação com inscritos

#### Comandos utilizados

Broker:

```powershell
go run .\cmd\broker
```

Subscriber 1:

```powershell
go run .\cmd\subscriber chat temperatura
```

Subscriber 2:

```powershell
go run .\cmd\subscriber alerta pedido
```

Publishers:

```powershell
go run .\cmd\publisher chat "{\"user\":\"ana\",\"msg\":\"oi\"}"
go run .\cmd\publisher temperatura "{\"valor\":28}"
go run .\cmd\publisher alerta "{\"nivel\":\"alto\"}"
go run .\cmd\publisher pedido "{\"id\":123,\"status\":\"novo\"}"
```

#### Resultado esperado

* o broker deve aceitar as conexões;
* os subscribers devem receber confirmação de inscrição;
* os publishers devem receber `ack published`;
* cada subscriber deve receber apenas as mensagens dos tópicos nos quais está inscrito.

#### Resultado obtido

As mensagens foram publicadas e entregues corretamente aos subscribers inscritos em cada tópico.

---

### Teste 2 - Remoção de inscrição (`unsubscribe`)

#### Comandos utilizados

Subscriber:

```text
unsub chat
```

#### Resultado esperado

* o broker deve confirmar a remoção da inscrição com `ack unsubscribed`.

#### Resultado obtido

A remoção da inscrição foi realizada com sucesso.

---

### Teste 3 - Publicação sem inscritos

#### Comandos utilizados

Após executar `unsub chat`, foi feita uma nova publicação:

```powershell
go run .\cmd\publisher chat "{\"user\":\"ana\",\"msg\":\"mensagem apos unsubscribe\"}"
```

#### Resultado esperado

* como não há mais inscritos em `chat`, a mensagem não deve ser encaminhada;
* o publisher deve receber resposta de descarte com `no subscribers`.

#### Resultado obtido

O broker retornou `ack discarded`, informando corretamente que não havia inscritos no tópico.

---

### Teste 4 - Múltiplos tópicos simultâneos

#### Comandos utilizados

Subscribers:

```powershell
go run .\cmd\subscriber chat temperatura
go run .\cmd\subscriber alerta pedido
```

Publishers:

```powershell
go run .\cmd\publisher chat "{\"user\":\"ana\",\"msg\":\"oi\"}"
go run .\cmd\publisher temperatura "{\"valor\":28}"
go run .\cmd\publisher alerta "{\"nivel\":\"alto\"}"
go run .\cmd\publisher pedido "{\"id\":123,\"status\":\"novo\"}"
```

#### Resultado esperado

* o Subscriber 1 deve receber apenas mensagens de `chat` e `temperatura`;
* o Subscriber 2 deve receber apenas mensagens de `alerta` e `pedido`.

#### Resultado obtido

Cada subscriber recebeu apenas as mensagens correspondentes aos seus tópicos, comprovando o correto roteamento.

---

### Teste 5 - Bufferização de mensagens

#### Comandos utilizados

Subscriber:

```powershell
go run .\cmd\subscriber chat
```

Publisher em sequência:

```powershell
1..10 | ForEach-Object { go run .\cmd\publisher chat "{\"user\":\"ana\",\"msg\":\"mensagem $_\"}" }
```

#### Resultado esperado

* o broker deve continuar aceitando novas publicações sem bloquear;
* o subscriber deve receber múltiplas mensagens do tópico `chat`.

#### Resultado obtido

O subscriber recebeu as mensagens publicadas em sequência, confirmando o uso de bufferização por tópico e o desacoplamento entre recebimento e encaminhamento.

---

## Comportamento do sistema

### Criação dinâmica de tópicos

Os tópicos são criados dinamicamente quando um cliente se inscreve em um tópico que ainda não existe.

### Remoção automática de tópicos

Quando não restam mais clientes inscritos em um tópico, ele é removido do broker.

### Publicação sem inscritos

Quando uma mensagem é publicada em um tópico sem inscritos, o broker não a encaminha e informa o descarte ao publisher.

### Encaminhamento para todos os inscritos

Quando existe pelo menos um cliente inscrito em um tópico, a mensagem publicada é encaminhada para todos os clientes inscritos nesse tópico.

## Bufferização e desacoplamento entre recebimento e encaminhamento

O broker utiliza **fila por tópico**, permitindo o desacoplamento entre:

* o recebimento das mensagens publicadas;
* o encaminhamento das mensagens aos subscribers.

Com isso:

* o broker não fica bloqueado durante o envio;
* múltiplas mensagens podem ser bufferizadas;
* o recebimento e o encaminhamento acontecem de forma independente.

## Status atual

Atualmente, o projeto já possui:

* broker funcional;
* biblioteca cliente funcional;
* publisher funcional;
* subscriber funcional;
* suporte a `subscribe`;
* suporte a `unsubscribe`;
* descarte de mensagens sem inscritos;
* cenário de teste com 4 tópicos validado.

## Limitações atuais

Nesta etapa, a arquitetura ainda utiliza uma única instância de broker para o funcionamento principal do sistema.

Assim, o requisito de **balanceamento de carga entre múltiplas instâncias do broker** ainda não foi implementado nesta versão e será tratado como próximo passo do projeto.

## Próximos passos

Os próximos passos planejados para o projeto são:

* adicionar suporte a múltiplas instâncias do broker;
* definir uma estratégia de balanceamento de carga;
* adaptar a biblioteca cliente para esconder do usuário final a existência de múltiplos brokers;
* manter transparência no uso das funções disponibilizadas pela biblioteca cliente.

## Conclusão

A implementação atual já demonstra o funcionamento básico de um middleware Publish/Subscribe com broker intermediário, biblioteca cliente e aplicações de exemplo, permitindo publicação, inscrição em tópicos, remoção de inscrição e troca de mensagens entre processos distribuídos.

A solução atende os requisitos centrais da primeira etapa do projeto e serve como base para a evolução futura da arquitetura com balanceamento entre múltiplas instâncias do broker.