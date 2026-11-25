from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg
import streamlit as st

from retail_stream.dashboard_data import euros, load_metrics, summarize

st.set_page_config(page_title="RetailStream", page_icon="🛒", layout="wide")
st.title("RetailStream")
st.caption("Corbeil-Essonnes · Commandes créées · Montants en euros · Horaires UTC")


@st.fragment(run_every="10s")
def dashboard():
    period_col, refresh_col = st.columns([4, 1])
    periods = {
        "Tout l’historique": None,
        "Dernière heure": 1,
        "Dernières 24 heures": 24,
        "7 derniers jours": 168,
    }
    period = period_col.selectbox("Période", list(periods))
    hours = periods[period]
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours) if hours else None
    refresh_col.button("Actualiser")
    try:
        frame = load_metrics(since)
    except psycopg.errors.UndefinedTable:
        st.info("Les agrégations ne sont pas encore initialisées. Lancez retail-aggregate.")
        return
    except psycopg.Error:
        st.error("PostgreSQL est indisponible. Vérifiez son démarrage et DATABASE_URL.")
        return
    if frame.empty:
        st.info(
            "Aucune commande pour Corbeil-Essonnes sur cette période. "
            "Lancez le pipeline ou élargissez la période."
        )
        return
    count, amount, items, average = summarize(frame)
    cards = st.columns(4)
    cards[0].metric("Commandes", f"{count:,}".replace(",", " "))
    cards[1].metric("Montant des commandes", euros(amount))
    cards[2].metric("Articles", f"{items:,}".replace(",", " "))
    cards[3].metric("Panier moyen", euros(average))
    frame = frame.copy()
    frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True)
    timeline = frame.groupby("window_start")[["order_count", "total_amount_cents"]].sum()
    timeline["Montant (€)"] = timeline["total_amount_cents"].astype(float) / 100
    timeline = timeline.rename(columns={"order_count": "Commandes"})
    timeline.index.name = "Minute (UTC)"
    left, right = st.columns(2)
    with left:
        st.subheader("Commandes par minute")
        st.bar_chart(timeline[["Commandes"]], color="#2563eb")
    with right:
        st.subheader("Montant par minute")
        st.bar_chart(timeline[["Montant (€)"]], color="#0d9488")
    st.subheader("Détail par minute")
    detail = frame.sort_values(["window_start", "warehouse_id"], ascending=[False, True]).copy()
    detail["Montant (€)"] = detail["total_amount_cents"].astype(float) / 100
    detail["Panier moyen (€)"] = detail["Montant (€)"] / detail["order_count"]
    detail["item_count"] = detail["item_count"].astype(int)
    detail["window_start"] = detail["window_start"].dt.strftime("%Y-%m-%d %H:%M")
    detail = detail.rename(
        columns={
            "window_start": "Minute (UTC)",
            "warehouse_id": "Entrepôt",
            "order_count": "Commandes",
            "item_count": "Articles",
        }
    ).drop(columns=["total_amount_cents", "Entrepôt"])
    st.dataframe(
        detail,
        hide_index=True,
        width="stretch",
        column_config={
            "Montant (€)": st.column_config.NumberColumn(format="%.2f"),
            "Panier moyen (€)": st.column_config.NumberColumn(format="%.2f"),
        },
    )
    latest = frame["window_start"].max().strftime("%Y-%m-%d %H:%M UTC")
    st.caption(
        f"Dernière fenêtre affichée : {latest} · Lecture : {now:%H:%M:%S} UTC "
        "· Actualisation : 10 s"
    )
    st.caption("Les fenêtres récentes peuvent encore évoluer avec les commandes retardées.")


dashboard()
