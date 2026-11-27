import pandas as pd
import psycopg
import streamlit as st

from retail_stream.catalog import connect
from retail_stream.config import DATABASE_URL


def load_business(dsn=DATABASE_URL):
    with connect(dsn) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute("SET LOCAL statement_timeout='5s'")
        stocks = conn.execute("""SELECT p.sku,p.name,p.status,s.physical,s.reserved,s.available,
            s.stock_version,s.event_time,
            i.version AS operational_version,
            CASE WHEN s.stock_version IS NULL OR s.stock_version<>i.version
                OR s.physical<>i.physical OR s.reserved<>i.reserved THEN true ELSE false END AS lag,
            i.reorder_point
            FROM products p JOIN inventory i USING(sku)
            LEFT JOIN inventory_projection s ON s.sku=i.sku AND s.warehouse_id=i.warehouse_id
            ORDER BY p.sku""").fetchall()
        metrics = conn.execute("SELECT * FROM business_metrics ORDER BY window_start").fetchall()
        replenishments = conn.execute("""SELECT *,
            EXTRACT(EPOCH FROM received_at-requested_at) AS delay_seconds
            FROM replenishment_projection ORDER BY requested_at DESC NULLS LAST""").fetchall()
        movements = conn.execute(
            "SELECT * FROM movement_metrics ORDER BY window_start DESC LIMIT 100"
        ).fetchall()
        pending = conn.execute(
            "SELECT count(*) AS n FROM outbox WHERE published_at IS NULL"
        ).fetchone()["n"]
    return stocks, metrics, replenishments, movements, pending


def render_business():
    try:
        stocks, metrics, replenishments, movements, pending = load_business()
    except psycopg.errors.UndefinedTable:
        st.info("Initialiser le catalogue avec retail-ops init.")
        return
    except psycopg.Error:
        st.error("PostgreSQL est indisponible.")
        return
    st.caption("Activité métier · Corbeil-Essonnes · Horaires UTC · Actualisation : 10 s")
    totals = {}
    for row in metrics:
        totals[row["event_type"]] = totals.get(row["event_type"], 0) + row["event_count"]
    cards = st.columns(4)
    for card, kind, label in zip(
        cards,
        ["order_created", "order_reserved", "order_rejected", "order_shipped"],
        ["Demandes", "Réservations réussies", "Refus", "Expéditions"],
        strict=True,
    ):
        card.metric(label, totals.get(kind, 0))
    if pending:
        st.info(f"{pending} événements en attente de publication Kafka.")
    if stocks:
        frame = pd.DataFrame(stocks)
        if frame["lag"].any():
            st.warning(
                "La projection analytique diffère du stock opérationnel pour "
                f"{int(frame['lag'].sum())} produits. Vérifier le relais et Spark."
            )
        st.subheader("Stocks observés par le pipeline")
        st.dataframe(frame, hide_index=True, width="stretch")
        available = frame["available"].notna()
        st.caption(
            f"{int((available & (frame['available'] == 0)).sum())} produits indisponibles · "
            f"{int((available & (frame['available'] <= frame['reorder_point'])).sum())} sous seuil"
        )
    else:
        st.info("Aucun produit initialisé.")
    if metrics:
        frame = pd.DataFrame(metrics)
        st.subheader("Événements de commande par minute")
        chart = frame.pivot_table(
            index="window_start",
            columns="event_type",
            values="event_count",
            aggfunc="sum",
            fill_value=0,
        )
        st.bar_chart(chart)
        st.subheader("Refus par motif")
        st.dataframe(frame[frame["event_type"] == "order_rejected"], hide_index=True)
    st.subheader("Réapprovisionnements observés")
    if replenishments:
        st.dataframe(pd.DataFrame(replenishments), hide_index=True)
    else:
        st.info("Aucun réapprovisionnement observé.")
    if movements:
        st.subheader("Mouvements par minute")
        st.dataframe(pd.DataFrame(movements), hide_index=True)
