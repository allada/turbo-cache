import { component$ } from "@builder.io/qwik";

import { Benefits } from "../sections/benefits";
import { Community } from "../sections/community";
import { Engineers } from "../sections/engineers";
import { Features } from "../sections/feature";
import { Hero } from "../sections/hero";
import { Testimonial } from "../sections/testimonials";

export const LandingPage = component$(() => {
  return (
    <main class="w-full z-20 bg-black font-nunito text-white">
      <Hero />
      <Testimonial />
      <Features />
      <Engineers />
      <div class="mx-auto left-0 right-0 w-9/11 flex justify-center items-center flex-col gap-4">
        <div class="flex justify-center w-full md:w-9/12 px-8 md:px-0 flex justify-center items-center pt-4 pb-12">
          <div class="w-full max-w-4xl aspect-video">
            <iframe
              title="Hermetic Toolchain Creation with Local Remote Execution (LRE) & Nix - Aaron Mondal, NativeLink"
              class="w-full h-full"
              src="https://www.youtube.com/embed/uokjTev8myk?rel=0"
              allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; fullscreen"
            />
          </div>
        </div>
      </div>
      <Benefits />
      <Community />
    </main>
  );
});
