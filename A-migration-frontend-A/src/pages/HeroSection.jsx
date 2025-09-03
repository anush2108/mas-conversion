import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Button } from '@carbon/react';
import Slider from "react-slick";
import "slick-carousel/slick/slick.css";
import "slick-carousel/slick/slick-theme.css";
import "../styles/Hero_Section.css";

import heroBg1 from '../assets/Hero.png';
import heroBg2 from '../assets/Banner.jpg';
import heroBg3 from '../assets/Banner.jpg';

const slides = [
  {
    title: "Maximo",
    subtitle: "Data Conversion Asset",
    description:
      "Seamlessly convert data from Oracle and MySQL databases to IBM Db2 with enhanced compatibility and minimal downtime.",
    image: heroBg1,
    cta: "Start Conversion",
  },
  {
    title: "AI Data Conversion ",
    subtitle: "Complexity Predictor",
    description:
      "Predict the complexity of data conversion project using a ML model based on key technical parameters.",
    image: heroBg2,
    cta: "Compute Complexity",
  },
  // {
  //   title: "Trusted Data Assurance",
  //   subtitle: "Migrate with Confidence",
  //   description:
  //     "Ensure end-to-end integrity with built-in validation—row count checks, column hashing, and precision-level auditing.",
  //   image: heroBg3,
  //   cta: "Start Migration",
  // },
];


const Hero_Section = ({ setDisableDbSelect, setIsBuildModalOpen, setIsAIMode, isLoggedIn }) => {
  const navigate = useNavigate();

  const handleMigrationClick = () => {
    if (!isLoggedIn) {
      navigate("/login");
    } else {
      setDisableDbSelect(false);
      setIsBuildModalOpen(true);
      if (setIsAIMode) setIsAIMode(false);
    }
  };

  const handleButtonClick = (index) => {
    if (index === 1) {
      if (!isLoggedIn) {
        navigate("/login");
      } else {
        setDisableDbSelect(false);
        if (setIsAIMode) setIsAIMode(true);
        setIsBuildModalOpen(true);
      }
    } else {
      handleMigrationClick();
    }
  };

  const settings = {
    dots: true,
    infinite: true,
    speed: 600,
    slidesToShow: 1,
    slidesToScroll: 1,
    autoplay: true,
    autoplaySpeed: 4000,
    arrows: false,
  };

  return (
    <section className="hero-container">
      <Slider {...settings} className="hero-slider">
        {slides.map((slide, i) => (
          <div className="hero-slide-wrapper" key={i}>
            <div
              className="hero-card"
              style={{ backgroundImage: `url(${slide.image})` }}
            >
              <div className="hero-overlay" />
              <div className="hero-content">
                <h1>{slide.title}</h1>
                {slide.subtitle && <h2>{slide.subtitle}</h2>}
                {slide.description && <p>{slide.description}</p>}
                <Button
                  kind="primary"
                  size="lg"
                  className="cta-button"
                  onClick={() => handleButtonClick(i)}
                >
                  {slide.cta}
                </Button>
              </div>
            </div>
          </div>
        ))}
      </Slider>
    </section>
  );
};


export default Hero_Section;
