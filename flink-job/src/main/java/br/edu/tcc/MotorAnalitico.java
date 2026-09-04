package br.edu.tcc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.Month;
import java.util.Arrays;
import java.util.List;
import java.time.Duration;

public class MotorAnalitico {

    private static final List<String> TOPICOS = Arrays.asList(
            "epidemiologia_ponta_grossa.public.notificacoes",
            "epidemiologia_curitiba.public.notificacoes",
            "epidemiologia_castro.public.notificacoes"
    );

    private static final String BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String GROUP_ID = "motor-analitico-epidemiologico";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(60000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPICOS)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
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
                                .withTimestampAssigner(
                                        new TimestampNotificacao()
                                )
                );

        DataStream<RegistroAlerta> registros =
                notificacoes.flatMap(new CriarChavesAnaliticas());

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
                .process(new AvaliarRegra());

        alertas.print("ALERTA-EPIDEMIOLOGICO");

        env.execute("Motor Analitico Epidemiologico");
    }

    public static class Notificacao implements Serializable {
        public long timestamp;
        public String municipio;
        public String codigoIbge;
        public String bairro;
        public String doenca;
        public String cid10;
        public int mes;

        public Notificacao() {
        }

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
            this.mes = Instant.ofEpochMilli(timestamp)
                    .atZone(ZoneId.of("America/Sao_Paulo"))
                    .getMonthValue();
        }
    }

    public static class RegistroAlerta implements Serializable {
        public String chave;
        public String tipo;
        public Notificacao notificacao;

        public RegistroAlerta() {
        }

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

    public static class ExtrairNotificacao
            implements FlatMapFunction<String, Notificacao> {

        private transient ObjectMapper mapper;

        @Override
        public void flatMap(
                String mensagem,
                Collector<Notificacao> out
        ) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }

            JsonNode raiz = mapper.readTree(mensagem);

            if (raiz == null || raiz.get("after") == null
                    || raiz.get("after").isNull()) {
                return;
            }

            JsonNode after = raiz.get("after");

            String municipio = texto(after, "municipio_residencia");
            String codigoIbge = texto(after, "codigo_ibge");
            String bairro = texto(after, "bairro");
            String doenca = texto(after, "doenca");
            String cid10 = texto(after, "cid_10");

            if (municipio.isEmpty()
                    || codigoIbge.isEmpty()
                    || doenca.isEmpty()) {
                return;
            }

            long dataMicros = numero(after, "dt_notificacao");

            if (dataMicros <= 0) {
                return;
            }

            long dataMillis = dataMicros / 1000L;

            out.collect(
                    new Notificacao(
                            dataMillis,
                            municipio,
                            codigoIbge,
                            bairro,
                            doenca.toUpperCase(),
                            cid10
                    )
            );
        }

        private String texto(JsonNode node, String campo) {
            JsonNode valor = node.get(campo);
            return valor == null || valor.isNull()
                    ? ""
                    : valor.asText().trim();
        }

        private long numero(JsonNode node, String campo) {
            JsonNode valor = node.get(campo);
            return valor == null || valor.isNull()
                    ? 0L
                    : valor.asLong();
        }
    }

    public static class TimestampNotificacao
            implements SerializableTimestampAssigner<Notificacao> {

        @Override
        public long extractTimestamp(
                Notificacao notificacao,
                long timestampAnterior
        ) {
            return notificacao.timestamp;
        }
    }

    public static class CriarChavesAnaliticas
            implements FlatMapFunction<Notificacao, RegistroAlerta> {

        @Override
        public void flatMap(
                Notificacao notificacao,
                Collector<RegistroAlerta> out
        ) {
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
                                    "DENGUE|BAIRRO|"
                                            + cidade
                                            + "|"
                                            + notificacao.bairro,
                                    "BAIRRO",
                                    notificacao
                            )
                    );
                }
            } else {
                out.collect(
                        new RegistroAlerta(
                                notificacao.doenca
                                        + "|CIDADE|"
                                        + cidade,
                                "CIDADE",
                                notificacao
                        )
                );
            }
        }
    }

    public static class AvaliarRegra
            extends ProcessWindowFunction<
                    RegistroAlerta,
                    String,
                    String,
                    TimeWindow
                    > {

        @Override
        public void process(
                String chave,
                Context contexto,
                Iterable<RegistroAlerta> eventos,
                Collector<String> out
        ) {
            int quantidade = 0;
            String doenca = "";
            String municipio = "";
            String bairro = "";
            int mes = 1;

            for (RegistroAlerta evento : eventos) {
                quantidade++;
                doenca = evento.notificacao.doenca;
                municipio = evento.notificacao.municipio;
                bairro = evento.notificacao.bairro;
                mes = evento.notificacao.mes;
            }

            int limiar = obterLimiar(doenca, eventoTipo(chave), mes);

            if (quantidade >= limiar) {
                out.collect(
                        String.format(
                                "ALERTA | doença=%s | município=%s"
                                        + " | bairro=%s | quantidade=%d"
                                        + " | limiar=%d | janela_inicio=%s"
                                        + " | janela_fim=%s | chave=%s",
                                doenca,
                                municipio,
                                bairro.isEmpty() ? "TODOS" : bairro,
                                quantidade,
                                limiar,
                                contexto.window().getStart(),
                                contexto.window().getEnd(),
                                chave
                        )
                );
            }
        }

        private String eventoTipo(String chave) {
            return chave.contains("|BAIRRO|")
                    ? "BAIRRO"
                    : "CIDADE";
        }

        private int obterLimiar(
                String doenca,
                String tipo,
                int mes
        ) {
            if ("DENGUE".equals(doenca)) {
                return "BAIRRO".equals(tipo) ? 5 : 10;
            }

            if ("MENINGITE".equals(doenca)) {
                return 3;
            }

            if ("INFLUENZA".equals(doenca)) {
                return estaNoOutonoOuInverno(mes) ? 200 : 120;
            }

            return Integer.MAX_VALUE;
        }

        private boolean estaNoOutonoOuInverno(int mes) {
            return mes == Month.MARCH.getValue()
                    || mes == Month.APRIL.getValue()
                    || mes == Month.MAY.getValue()
                    || mes == Month.JUNE.getValue()
                    || mes == Month.JULY.getValue()
                    || mes == Month.AUGUST.getValue();
        }
    }
}
