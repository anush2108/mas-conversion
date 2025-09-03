import React, { useState, useRef } from "react";
import { Grid, Column } from "@carbon/react";
import { Add, Subtract } from "@carbon/icons-react";
import Header from "./Header";
import Footer from "./Footer";
import "../styles/MaximoDataFAQ.css";

const faqs = [
  {
    question: "What is Maximo Data Conversion Asset?",
    answer:
      "Maximo Data Conversion Asset efficiently transfers asset and related records into IBM Maximo, ensuring data integrity and minimizing manual work.",
  },
  {
    question: "Why should I use automated data Conversion?",
    answer:
      "Automated Conversion reduces manual errors, increases consistency, and speeds up onboarding of assets so your operations run smoothly.",
  },
  {
    question: "Which data formats are supported?",
    answer:
      "CSV, Excel, database exports, and custom formats are supported. You can extend support with custom parsers as needed.",
  },
  {
    question: "Is my data secure during Conversion?",
    answer:
      "Yes. All data transfers use encrypted connections, and it’s recommended to run Conversions on secure, private networks.",
  },
  {
    question: "How do I resolve Conversion errors?",
    answer:
      "The Conversion dashboard provides error logs and highlights problematic records. Built-in tools allow for validation and corrections.",
  },
  {
    question: "Is support available?",
    answer:
      "Yes. Technical support is available during the project and for 30 days post-conversion. Extended support is available on request.",
  },
];

const MaximoDataFAQ = () => {
  const [openIndex, setOpenIndex] = useState(null);
  const answerRefs = useRef([]);

  const toggleFAQ = (idx) => {
    setOpenIndex(openIndex === idx ? null : idx);
  };

  return (
    <>
      <Header />
      <main className="maximo-faq-main">
        <section className="maximo-faq-outer">
          <h1 className="cds--type-productive-heading-04 maximo-faq-title">
            Maximo Data Conversion Asset – FAQs
          </h1>
          <p className="cds--type-body-long-01 maximo-faq-subtitle">
            Find answers to the most common questions about converting assets into
            IBM Maximo.
          </p>
          <div className="maximo-faq-list" aria-label="FAQs">
            {faqs.map((faq, idx) => (
              <div className="maximo-faq-row" key={idx}>
                <button
                  type="button"
                  className={`maximo-faq-question${openIndex === idx ? " open" : ""}`}
                  onClick={() => toggleFAQ(idx)}
                  aria-expanded={openIndex === idx}
                  aria-controls={`faq-ans-${idx}`}
                >
                  <span className="maximo-faq-qtext">{faq.question}</span>
                  {openIndex === idx ? (
                    <Subtract size={20} aria-label="Collapse answer" />
                  ) : (
                    <Add size={20} aria-label="Expand answer" />
                  )}
                </button>
                <div
                  id={`faq-ans-${idx}`}
                  ref={(el) => (answerRefs.current[idx] = el)}
                  className={`maximo-faq-answer${openIndex === idx ? " open" : ""}`}
                  style={{
                    maxHeight:
                      openIndex === idx && answerRefs.current[idx]
                        ? answerRefs.current[idx].scrollHeight + "px"
                        : "0px",
                  }}
                  aria-hidden={openIndex !== idx}
                >
                  <div className="maximo-faq-answer-content cds--type-body-long-01">
                    {faq.answer}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>
      </main>
      <Footer />
    </>
  );
};

export default MaximoDataFAQ;
