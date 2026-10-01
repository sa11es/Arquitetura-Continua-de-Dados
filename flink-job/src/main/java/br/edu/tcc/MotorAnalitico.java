package br.edu.tcc;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MotorAnalitico {

    private static final String BOOTSTRAP_SERVERS = "kafka:29092";

    private static final String GROUP_ID = "motor-analitico-epidemiologico-v2";

    private static final long JANELA_MS = Duration.ofHours(24).toMillis();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // Checkpoint reduzido de 60s para 20s: em execucoes de teste de 5 minutos
        // (compressao 24h->5min), um intervalo de 60s concentra poucos pontos de
        // pausa distribuidos de forma grosseira ao longo da execucao, podendo
        // aparecer como "solavancos" periodicos na serie de latencia coletada.
        env.enableCheckpointing(20000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopicPattern(Pattern.compile("epidemiologia_.*\\.public\\.notificacoes"))
                .setGroupId(GROUP_ID)
                // ALTERADO PARA OS TESTES DE DESEMPENHO: latest() em vez de earliest().
                // Cada execucao de teste deve medir SOMENTE os eventos gerados
                // naquela rodada, sem reprocessar o historico acumulado de
                // execucoes anteriores (o que inflaria throughput e distorceria
                // a latencia com eventos "antigos" sendo processados de uma vez).
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // Sem isso, o Flink descobre os topicos que casam com o padrao
                // UMA UNICA VEZ, no instante em que o job sobe. Reavalia a cada 10s.
                .setProperty("partition.discovery.interval.ms", "10000")
                .build();

        DataStreamSource<String> mensagens = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Fonte Kafka CDC"
        );

        DataStream<Notificacao> notificacoes = mensagens
                .flatMap(new ExtrairNotificacao())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Notificacao>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withIdleness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new TimestampNotificacao())
                );

        DataStream<RegistroAlerta> registros = notificacoes.flatMap(new CriarChavesAnaliticas());

        DataStream<String> alertas = registros
                .keyBy(registro -> registro.chave, TypeInformation.of(String.class))
                .process(new ContagemTempoReal());

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("epidemiologia.alertas")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        alertas.sinkTo(kafkaSink).name("Sink Kafka Alertas");
        alertas.print("CONTAGEM-EPIDEMIOLOGICA");

        env.execute("Motor Analitico Epidemiologico");
    }

    public static class Notificacao implements Serializable {
        public long timestamp;          // Event Time (dt_notificacao/dt_sintomas) - usado pelas janelas do Flink
        public long sourceTsMs;         // source.ts_ms do Debezium (commit no WAL, UTC real) - usado para medir Le2e
        public long idNotificacao;      // id_notificacao da tabela - so para conferencia/depuracao pontual
        public String municipio;
        public String codigoIbge;
        public String bairro;
        public String doenca;
        public String cid10;

        public Notificacao() {}

        public Notificacao(long timestamp, long sourceTsMs, long idNotificacao, String municipio,
                            String codigoIbge, String bairro, String doenca, String cid10) {
            this.timestamp = timestamp;
            this.sourceTsMs = sourceTsMs;
            this.idNotificacao = idNotificacao;
            this.municipio = municipio;
            this.codigoIbge = codigoIbge;
            this.bairro = bairro;
            this.doenca = doenca;
            this.cid10 = cid10;
        }
    }

    public static class RegistroAlerta implements Serializable {
        public String chave;
        public String tipo;
        public Notificacao notificacao;

        public RegistroAlerta() {}

        public RegistroAlerta(String chave, String tipo, Notificacao notificacao) {
            this.chave = chave;
            this.tipo = tipo;
            this.notificacao = notificacao;
        }
    }

    /**
     * Contador estático simples só para diagnóstico via aba "Stdout" da Web UI.
     * A cada 20 mensagens recebidas do Kafka, imprime um resumo — isso confirma
     * visualmente se o operador está de fato recebendo e conseguindo extrair
     * as notificações, sem depender só de println por evento (que pode
     * inundar o log e, sob carga do cenário de Surto, virar gargalo).
     */
    public static class ExtrairNotificacao implements FlatMapFunction<String, Notificacao> {
        private transient ObjectMapper mapper;
        private static final AtomicLong recebidas = new AtomicLong(0);
        private static final AtomicLong extraidas = new AtomicLong(0);
        private static final AtomicLong descartadas = new AtomicLong(0);

        @Override
        public void flatMap(String mensagem, Collector<Notificacao> out) {
            long total = recebidas.incrementAndGet();
            if (total % 20 == 1) {
                System.out.println("[DIAGNOSTICO] Mensagens recebidas do Kafka ate agora: " + total);
            }

            try {
                if (mapper == null) {
                    mapper = new ObjectMapper();
                }

                JsonNode raiz = mapper.readTree(mensagem);
                if (raiz == null || !raiz.hasNonNull("after")) {
                    descartadas.incrementAndGet();
                    return; // Ignora deletes ou mensagens vazias (after == null)
                }

                JsonNode after = raiz.get("after");

                String municipio = texto(after, "municipio_residencia").toUpperCase();
                String codigoIbge = texto(after, "codigo_ibge");
                String bairro = texto(after, "bairro").toUpperCase();
                String doenca = texto(after, "doenca").toUpperCase();
                String cid10 = texto(after, "cid_10");

                if (municipio.isEmpty() || codigoIbge.isEmpty() || doenca.isEmpty()) {
                    System.out.println("[DIAGNOSTICO] Descartada por campo obrigatorio vazio: "
                            + "municipio='" + municipio + "' codigoIbge='" + codigoIbge + "' doenca='" + doenca + "'");
                    descartadas.incrementAndGet();
                    return;
                }

                long dataValue = numero(after, "dt_notificacao");
                if (dataValue <= 0) {
                    dataValue = numero(after, "dt_sintomas");
                }

                long dataMillis;
                if (dataValue <= 0) {
                    dataMillis = System.currentTimeMillis();
                } else if (dataValue < 100000000000L) {
                    dataMillis = dataValue * 86400000L; // DATE (dias) -> ms
                } else if (dataValue > 1000000000000000L) {
                    dataMillis = dataValue / 1000L; // MicroTimestamp (us) -> ms
                } else {
                    dataMillis = dataValue; // já em ms
                }

                // source.ts_ms: instante de commit no WAL do Postgres, capturado
                // pelo proprio Debezium em UTC real (epoch millis). Ao contrario de
                // dt_notificacao (TIMESTAMP sem fuso, sujeito a ambiguidade de
                // interpretacao), este campo nao depende de conversao de fuso
                // horario e e a referencia correta para medir Le2e = Tdestino - Torigem.
                JsonNode sourceNode = raiz.get("source");
                long sourceTsMs = (sourceNode != null && sourceNode.hasNonNull("ts_ms"))
                        ? sourceNode.get("ts_ms").asLong(0L)
                        : 0L;

                long idNotificacao = numero(after, "id_notificacao");

                out.collect(new Notificacao(dataMillis, sourceTsMs, idNotificacao, municipio, codigoIbge, bairro, doenca, cid10));

                long okTotal = extraidas.incrementAndGet();
                if (okTotal % 20 == 1) {
                    System.out.println("[DIAGNOSTICO] Notificacoes extraidas com sucesso ate agora: " + okTotal);
                }

            } catch (Exception e) {
                descartadas.incrementAndGet();
                System.out.println("[ERRO] Falha ao processar JSON: " + mensagem);
                e.printStackTrace(System.out); // System.out garante aparecer na aba Stdout
            }
        }

        private String texto(JsonNode node, String campo) {
            JsonNode valor = node.get(campo);
            return valor == null || valor.isNull() ? "" : valor.asText().trim();
        }

        private long numero(JsonNode node, String campo) {
            JsonNode valor = node.get(campo);
            return valor == null || valor.isNull() ? 0L : valor.asLong();
        }
    }

    public static class TimestampNotificacao implements SerializableTimestampAssigner<Notificacao> {
        @Override
        public long extractTimestamp(Notificacao notificacao, long timestampAnterior) {
            return notificacao.timestamp;
        }
    }

    public static class CriarChavesAnaliticas implements FlatMapFunction<Notificacao, RegistroAlerta> {
        @Override
        public void flatMap(Notificacao notificacao, Collector<RegistroAlerta> out) {
            String cidade = notificacao.codigoIbge;
            if ("DENGUE".equals(notificacao.doenca)) {
                out.collect(new RegistroAlerta("DENGUE|CIDADE|" + cidade, "CIDADE", notificacao));
                if (!notificacao.bairro.isEmpty()) {
                    out.collect(new RegistroAlerta("DENGUE|BAIRRO|" + cidade + "|" + notificacao.bairro, "BAIRRO", notificacao));
                }
            } else {
                out.collect(new RegistroAlerta(notificacao.doenca + "|CIDADE|" + cidade, "CIDADE", notificacao));
            }
        }
    }

    public static class ContagemTempoReal extends KeyedProcessFunction<String, RegistroAlerta, String> {
        private transient ListState<Long> timestampsAtivos;
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parametros) {
            timestampsAtivos = getRuntimeContext().getListState(new ListStateDescriptor<>("timestamps-ativos", Long.class));
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(RegistroAlerta registro, Context ctx, Collector<String> out) throws Exception {
            long eventTime = ctx.timestamp();
            timestampsAtivos.add(eventTime);
            ctx.timerService().registerEventTimeTimer(eventTime + JANELA_MS);
            emitirContagem(ctx.getCurrentKey(), registro, out);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<Long> restantes = new ArrayList<>();
            for (Long ts : timestampsAtivos.get()) {
                if (ts > timestamp - JANELA_MS) {
                    restantes.add(ts);
                }
            }
            timestampsAtivos.update(restantes);
        }

        private void emitirContagem(String chave, RegistroAlerta registro, Collector<String> out) throws Exception {
            int quantidade = 0;
            for (Long ignored : timestampsAtivos.get()) {
                quantidade++;
            }

            String tipo = chave.contains("|BAIRRO|") ? "BAIRRO" : "CIDADE";
            String bairro = registro.notificacao.bairro;

            Map<String, Object> payload = new HashMap<>();
            payload.put("chave", chave);
            payload.put("doenca", registro.notificacao.doenca);
            payload.put("municipio", registro.notificacao.municipio);
            payload.put("codigo_ibge", registro.notificacao.codigoIbge);
            payload.put("bairro", bairro.isEmpty() ? "TODOS" : bairro);
            payload.put("tipo_agregacao", tipo);
            payload.put("quantidade_casos", quantidade);
            payload.put("atualizado_em", Instant.now().toString());
            // Origem real (UTC, via Debezium) para o calculo de Le2e = Tdestino - Torigem.
            payload.put("origem_ts_ms", registro.notificacao.sourceTsMs);
            // Apenas para conferencia pontual/depuracao - nao usado nos calculos agregados.
            payload.put("id_ultima_notificacao", registro.notificacao.idNotificacao);

            String json = mapper.writeValueAsString(payload);
            out.collect(json);
        }
    }
}