package br.edu.tcc;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MotorAnalitico {

    private static final List<String> TOPICOS = Arrays.asList(
            "epidemiologia_ponta_grossa.public.notificacoes",
            "epidemiologia_curitiba.public.notificacoes",
            "epidemiologia_castro.public.notificacoes"
    );
    private static final String BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String GROUP_ID = "motor-analitico-epidemiologico";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPICOS)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
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
                                .withTimestampAssigner(new TimestampNotificacao())
                );

        DataStream<RegistroAlerta> registros = notificacoes.flatMap(new CriarChavesAnaliticas());

        DataStream<String> alertas = registros
                .keyBy(
                        registro -> registro.chave,
                        TypeInformation.of(String.class)
                )
                .window(
                        SlidingEventTimeWindows.of(
                                Time.hours(24),
                                Time.minutes(5)
                        )
                )
                .process(new ContarCasosJanela()); // Processador simplificado para contagem

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
        public long timestamp;
        public String municipio;
        public String codigoIbge;
        public String bairro;
        public String doenca;
        public String cid10;

        public Notificacao() {}

        public Notificacao(
                long timestamp,
                String municipio,
                String codigoIbge,
                String bairro,
                String doenca,
                String cid10
        ) {
            this.timestamp = timestamp;
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

        public RegistroAlerta(
                String chave,
                String tipo,
                Notificacao notificacao
        ) {
            this.chave = chave;
            this.tipo = tipo;
            this.notificacao = notificacao;
        }
    }

    public static class ExtrairNotificacao implements FlatMapFunction<String, Notificacao> {
        private transient ObjectMapper mapper;

        @Override
        public void flatMap(String mensagem, Collector<Notificacao> out) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            JsonNode raiz = mapper.readTree(mensagem);
            if (raiz == null || raiz.get("after") == null || raiz.get("after").isNull()) {
                return;
            }
            JsonNode after = raiz.get("after");

            String municipio = texto(after, "municipio_residencia").toUpperCase();
            String codigoIbge = texto(after, "codigo_ibge");
            String bairro = texto(after, "bairro").toUpperCase();
            String doenca = texto(after, "doenca").toUpperCase();
            String cid10 = texto(after, "cid_10");

            if (municipio.isEmpty() || codigoIbge.isEmpty() || doenca.isEmpty()) {
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
                dataMillis = dataValue * 86400000L;
            } else if (dataValue > 1000000000000000L) {
                dataMillis = dataValue / 1000L;
            } else {
                dataMillis = dataValue;
            }

            out.collect(
                    new Notificacao(
                            dataMillis,
                            municipio,
                            codigoIbge,
                            bairro,
                            doenca,
                            cid10
                    )
            );
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
                out.collect(
                        new RegistroAlerta(
                                "DENGUE|CIDADE|" + cidade,
                                "CIDADE",
                                notificacao
                        )
                );
                if (!notificacao.bairro.isEmpty()) {
                    out.collect(
                            new RegistroAlerta(
                                    "DENGUE|BAIRRO|" + cidade + "|" + notificacao.bairro,
                                    "BAIRRO",
                                    notificacao
                            )
                    );
                }
            } else {
                out.collect(
                        new RegistroAlerta(
                                notificacao.doenca + "|CIDADE|" + cidade,
                                "CIDADE",
                                notificacao
                        )
                );
            }
        }
    }

    /**
     * ProcessWindowFunction focado exclusivamente na contagem dos casos acumulados na janela.
     */
    public static class ContarCasosJanela extends ProcessWindowFunction<RegistroAlerta, String, String, TimeWindow> {

        private transient ObjectMapper mapper;

        private static final DateTimeFormatter FMT_JANELA =
                DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm")
                        .withZone(ZoneId.of("America/Sao_Paulo"));

        @Override
        public void process(
                String chave,
                Context contexto,
                Iterable<RegistroAlerta> eventos,
                Collector<String> out
        ) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }

            int quantidadeCasos = 0;
            String doenca = "";
            String municipio = "";
            String codigoIbge = "";
            String bairro = "";

            for (RegistroAlerta evento : eventos) {
                quantidadeCasos++;
                doenca = evento.notificacao.doenca;
                municipio = evento.notificacao.municipio;
                codigoIbge = evento.notificacao.codigoIbge;
                bairro = evento.notificacao.bairro;
            }

            if (quantidadeCasos > 0) {
                String tipo = chave.contains("|BAIRRO|") ? "BAIRRO" : "CIDADE";
                String janelaInicio = FMT_JANELA.format(Instant.ofEpochMilli(contexto.window().getStart()));
                String janelaFim = FMT_JANELA.format(Instant.ofEpochMilli(contexto.window().getEnd()));

                Map<String, Object> payload = new HashMap<>();
                payload.put("chave", chave);
                payload.put("doenca", doenca);
                payload.put("municipio", municipio);
                payload.put("codigo_ibge", codigoIbge);
                payload.put("bairro", bairro.isEmpty() ? "TODOS" : bairro);
                payload.put("tipo_agregacao", tipo);
                payload.put("quantidade_casos", quantidadeCasos);
                payload.put("janela_inicio", janelaInicio);
                payload.put("janela_fim", janelaFim);

                out.collect(mapper.writeValueAsString(payload));
            }
        }
    }
}